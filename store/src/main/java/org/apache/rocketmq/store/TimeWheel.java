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
package org.apache.rocketmq.store;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * add-huqi timeWheel 工具
 */
public class TimeWheel {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TimeWheel.class);

    private long start;
    private ArrayList<List<Object>> data;
    private int index;

    public TimeWheel(MessageStoreConfig config, long start, long tt) {
        this.start = start;
        this.data = new ArrayList<>((int) config.getDelayLogInterval());
        for (int i = 0; i < config.getDelayLogInterval(); i++) {
            this.data.add(null);
        }

        if (tt > 0) {
            this.index = (int) ((tt - this.start) / 1000);
        } else {
            this.index = -1;
        }
        if (this.index < -1) {
            this.index = -1;
        }
    }


    public List<Object> get(long time) {
        int shouldlIx = (int) ((time - start) / 1000);
        LinkedList<Object> datas = new LinkedList<>();
        while (shouldlIx > index) {
            index++;
            if (index >= this.data.size()) {
                return datas;
            }
            if (this.data.get(index) != null) {
                datas.addAll(this.data.get(index));
            }
        }
        return datas;
    }

    public void put(long time, Object object) {
        int shouldlIx = (int) ((time - start) / 1000);
        if (shouldlIx <= index) {
            logger.error("error,shouldlIx:{},index:{},will exit", shouldlIx, index);
            System.exit(0);
        }
        List<Object> datas = data.get(shouldlIx);
        if (datas == null) {
            datas = new LinkedList<Object>();
            data.set(shouldlIx, datas);
        }
        datas.add(object);
    }


    public void setStartIndex(long start) {
        this.index = (int) ((start - this.start) / 1000);
    }


    public int getSize() {
        int size = 0;
        for (int i = 0; i < this.data.size(); i++) {
            List<Object> objects = this.data.get(i);
            if (objects != null) {
                size += objects.size();
            }
        }
        return size;
    }

}
