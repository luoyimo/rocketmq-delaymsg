/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    public static final String SCHEDULE_TOPIC_DELAY = "SCHEDULE_CONSUME_TOPIC_XXXX";


    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private ConsumeQueue consumeQueue;
    private final DelayMessageStore delayMessageStore;
    private ExecutorService executor;
    private MappedByteBuffer mappedByteBuffer;

    /**
     * current execute time
     */
    long currentExecuteTime = System.currentTimeMillis();

    /**
     * is shutdown?
     */

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * now timeWheel ID
     */
    private long nowQueueId;
    /**
     * now timeWheel
     */
    private TimeWheel nowTimeWheel;
    /**
     * next timeWheel ID
     */
    private long nextQueueId;
    /**
     * next timewheel
     */
    private TimeWheel nextTimeWheel;

    /**
     * read data to timewheel, file position.
     */
    private long nextPosition = 0;


    /**
     * consume queue startIndex
     */
    private AtomicInteger startIndex;

    /**
     * last flush time
     */
    long lastFlushTime = System.currentTimeMillis();


    private volatile int delaysize = 0;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.delayMessageStore = new DelayMessageStore(defaultMessageStore);
        this.executor = Executors.newSingleThreadExecutor();
        consumeQueue = new ConsumeQueue(SCHEDULE_TOPIC_DELAY,
                1, defaultMessageStore.getMessageStoreConfig().getStorePathDelayLog(),
                defaultMessageStore.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
                defaultMessageStore);

    }


    public void putMessagePositionInfo(DispatchRequest req) {
        final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            int ll = this.getDelayTimeLevel(req);
            if (ll > 0) {
                this.consumeQueue.putMessagePositionInfoWrapper(req);
                this.consumeQueue.flush(8);
            }
        }
    }

    public void destroy() {
        this.delayMessageStore.destroy();
        this.consumeQueue.destroy();
        this.shutdown();
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }


    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(int delayLevel, long storeTimestamp) {
        return delayLevel * 1000 + storeTimestamp;
    }


    public void run() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
        if (this.mappedByteBuffer.getLong(8) != 0) {
            this.currentExecuteTime = this.mappedByteBuffer.getLong(8);
            if (this.nowTimeWheel != null && this.currentExecuteTime != 0) {
                this.nowTimeWheel.setStartIndex(this.currentExecuteTime);
            }
        }

        log.info("nowTimeWheel size:{}", nowTimeWheel.getSize());

        while (!shutdown.get()) {
            currentExecuteTime = currentExecuteTime + 1000;
            if (System.currentTimeMillis() < currentExecuteTime) {
                currentExecuteTime = System.currentTimeMillis();
            }
            if (System.currentTimeMillis() - lastFlushTime >= this.defaultMessageStore.getMessageStoreConfig().getFlushDelayLogInterval()) {
                lastFlushTime = System.currentTimeMillis();
                this.persist();
            }

            SelectMappedBufferResult smb = this.consumeQueue.getIndexBuffer(startIndex.get());

            if (smb != null && smb.getByteBuffer() != null) {
                ByteBuffer buffer = smb.getByteBuffer();
                long phyOffset = buffer.getLong();
                int bufferSize = buffer.getInt();
                MessageExt msgExt = defaultMessageStore.lookMessageByOffset(
                        phyOffset, bufferSize);
                if (bufferSize > 0 && msgExt != null) {
                    this.putMessageToTimeWheel(currentExecuteTime, msgExt);
                    startIndex.incrementAndGet();
                    this.mappedByteBuffer.putLong(0, startIndex.get());
                }
                log.info(" MessageExt:{}, startIndex:{},phyOffset:{},bufferSize:{}", msgExt, (startIndex), phyOffset, bufferSize);
            }

            if (this.nowTimeWheel != null) {
                List<Object> obs = this.nowTimeWheel.get(currentExecuteTime);
                if (obs != null && !obs.isEmpty()) {
                    for (Object o : obs) {
                        MessageExt msgE = (MessageExt) o;
                        MessageExtBrokerInner msgInner = DeliverDelayedMessageTimerTask.messageTimeup(msgE);
                        clearMsg(msgInner);
                        PutMessageResult putMessageResult = defaultMessageStore.putMessage(msgInner);
                        if (isPutMessageFail(putMessageResult)) {
                            log.error("ScheduleMessageService, 延时消息延迟结束,重新入队失败,{},topic: {} msgId {}", putMessageResult,
                                    msgInner.getTopic(), msgInner.getMsgId());

                        }
                    }
                }

            }
            this.mappedByteBuffer.putLong(8, currentExecuteTime);


            long nowId = currentExecuteTime / 1000 / this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval();
            if (nowId > this.nowQueueId && this.nextQueueId != -1) {
                clearExpiredLog(nowQueueId);
                this.nowQueueId = this.nextQueueId;
                this.nowTimeWheel = this.nextTimeWheel;
                this.nextTimeWheel = null;
                this.nextQueueId = -1;
                this.nextPosition = 0;
            }

            if ((nowQueueId + 1) * 1000 * this.defaultMessageStore
                    .getMessageStoreConfig().getDelayLogInterval() - currentExecuteTime <=
                    this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval() / 5 * 1000
                    && this.nextQueueId == -1) {
                if (this.nextTimeWheel == null) {
                    this.nextTimeWheel = new TimeWheel(this.defaultMessageStore.getMessageStoreConfig(),
                            (nowQueueId + 1) * 1000 * this.defaultMessageStore.getMessageStoreConfig()
                                    .getDelayLogInterval(), this.currentExecuteTime);

                }
                if (this.nextQueueId == -1) {
                    boolean me = loadNextTimeWheel(nowQueueId + 1, this.nextTimeWheel);
                    if (!me) {
                        this.nextQueueId = nowQueueId + 1;
                        this.nextPosition = 0;
                    }
                }
            }
            this.persist();
        }
    }


    private void putMessageToTimeWheel(long time, MessageExt msgExt) {
        int ll = msgExt.getDelayTimeLevel();
        if (ll > 0) {
            long queueId = ((ll * 1000 + msgExt.getBornTimestamp()) / 1000) / this.defaultMessageStore
                    .getMessageStoreConfig().getDelayLogInterval();
            //消息已超时
            log.info("当前队列id:[{}],延时消息队列id:[{}],下一个消息队列:[{}]", nowQueueId, queueId, nextQueueId);
            if ((time >= (ll * 1000 + msgExt.getBornTimestamp())) || (queueId < nowQueueId) ||
                    (time / 1000 >= (ll * 1000 + msgExt.getBornTimestamp()) / 1000)) {
                // expired
                MessageExtBrokerInner msgInner = DeliverDelayedMessageTimerTask.messageTimeup(msgExt);
                clearMsg(msgInner);
                PutMessageResult putMessageResult = defaultMessageStore.putMessage(msgInner);
                if (isPutMessageFail(putMessageResult)) {
                    log.error("延时消息已超时，重新入队失败, topic: {} msgId {}",
                            putMessageResult, msgExt.getTopic(), msgExt.getMsgId());
                }
                return;
            }

            //延时时间在当前timeWheel
            if (queueId == nowQueueId) {
                log.info("put timewheel size :{}", (this.delaysize++));
                nowTimeWheel.put(ll * 1000 + msgExt.getBornTimestamp(), msgExt);
                PutMessageResult putMessageResult = delayMessageStore
                        .putMessage(DeliverDelayedMessageTimerTask.messageTimeup(msgExt));
                if (isPutMessageFail(putMessageResult)) {
                    log.error("ScheduleMessageService, 延时消息写入DelayLog失败,{},topic: {} msgId {}", putMessageResult,
                            msgExt.getTopic(), msgExt.getMsgId());
                }
                return;
            }

            //延时时间不在当前timeWheel
            if (queueId > nowQueueId) {
                PutMessageResult putMessageResult = this.delayMessageStore.putMessage(DeliverDelayedMessageTimerTask.messageTimeup(msgExt));
                if (isPutMessageFail(putMessageResult)) {
                    // XXX: warn and notify me
                    log.error("ScheduleMessageService,延时消息不在当前timeWheel,写入DelayLog失败,failed,{},topic: {} msgId {}", putMessageResult,
                            msgExt.getTopic(), msgExt.getMsgId());
                }
            }
        }
    }

    private boolean isPutMessageFail(PutMessageResult putMessageResult) {
        return putMessageResult == null || putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK;
    }

    /**
     * 清除延迟属性  重新入队
     *
     * @param msgInner
     */
    private void clearMsg(MessageExtBrokerInner msgInner) {
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

    }

    /**
     * 移除过期文件
     *
     * @param queueId
     */
    private void clearExpiredLog(long queueId) {
        this.persist();
        this.delayMessageStore.remove(queueId);
        this.consumeQueue.deleteExpiredFileByIndex(startIndex.get());
    }

    public void persist() {
        this.mappedByteBuffer.force();
        this.consumeQueue.flush(0);
        this.delayMessageStore.flush();
    }

    public void start() {
        shutdown.compareAndSet(false, false);
        Long bufferTime = this.mappedByteBuffer.getLong(8);
        log.info("start-bufferTime:{}", bufferTime);
        long nowTime = System.currentTimeMillis();
        if (bufferTime == null) {
            File dirLogic = new File(this.defaultMessageStore.getMessageStoreConfig().getStorePathDelayLog());
            File[] fileQueueIdList = dirLogic.listFiles();
            if (fileQueueIdList != null) {
                Arrays.sort(fileQueueIdList);
                for (File fileQueueId : fileQueueIdList) {
                    long queueId;
                    try {
                        queueId = Integer.parseInt(fileQueueId.getName());
                    } catch (NumberFormatException e) {
                        log.error(e.getMessage(), e);
                        continue;
                    }
                    this.currentExecuteTime = queueId * this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval() * 1000;
                    initNowTimeWheel(queueId);
                }
            } else {
                /**
                 * edit-huqi 直接写死消息队列
                 */
                long queueId = nowTime / 1000 / this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval();
                initNowTimeWheel(queueId);
            }

        } else {
            /**
             * edit-huqi 直接写死消息队列
             */
            long queueId = bufferTime / 1000 / this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval();
            initNowTimeWheel(queueId);
        }

        executor.submit(() ->
                run());
//        this.executor.execute(this);
    }

    private void initNowTimeWheel(long queueId) {
        nowTimeWheel = new TimeWheel(this.defaultMessageStore.getMessageStoreConfig(),
                queueId * this.defaultMessageStore.getMessageStoreConfig()
                        .getDelayLogInterval() * 1000, 0);
        while (true) {
            boolean re = this.loadNextTimeWheel(queueId, nowTimeWheel);
            if (!re) {
                break;
            }
        }
        nowQueueId = queueId;
        nextQueueId = -1;
        nextTimeWheel = null;
        nextPosition = 0;
    }

    private boolean loadNextTimeWheel(long l, TimeWheel tw) {
        for (int i = 0; i < this.delayMessageStore.getDefaultMessageStore().getMessageStoreConfig().getReadDelayLogOnce(); i++) {
            MessageExt me = this.delayMessageStore.lookMessageByOffset(l, nextPosition);
            if (me == null) {
                return false;
            }
            nextPosition = nextPosition + me.getStoreSize();
            tw.put(me.getBornTimestamp() + me.getDelayTimeLevel() * 1000, me);
        }
        return true;
    }

    public void shutdown() {
        this.persist();
        shutdown.compareAndSet(false, true);
        this.executor.shutdown();
        try {
            this.executor.awaitTermination(90, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    public boolean isStarted() {
        return started.get();
    }


    public String encode() {
        return this.encode(false);
    }


    public boolean load() {

        this.delayMessageStore.load();
        this.consumeQueue.load();
        this.consumeQueue.recover();
        File file = new File(this.configFilePath());

        MappedFile.ensureDirOK(file.getParent());

        try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024L * 4);
            this.startIndex = new AtomicInteger((int) this.mappedByteBuffer.getLong(0));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }

        return true;

    }


    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }


    public int getDelayTimeLevel(DispatchRequest req) {
        Map<String, String> pros = req.getPropertiesMap();
        if (pros != null) {
            String t = pros.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            if (t != null) {
                return Integer.parseInt(t);
            }
        }
        return 0;
    }

    static class DeliverDelayedMessageTimerTask {

        private DeliverDelayedMessageTimerTask() {

        }

        public static MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
            msgInner.setWaitStoreMsgOK(false);
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);
            return msgInner;
        }
    }
}
