<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<template>
  <div>
    <el-row :gutter="20">
      <el-col :span="4">
        <router-link to="/shuffleserverpage/activeNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Active</span>
              </div>
            </template>
            <div class="activenode">{{ dataList.allshuffleServerSize.ACTIVE ?? 0 }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/decommissioningNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Decommissioning</span>
              </div>
            </template>
            <div class="decommissioningnode">
              {{ dataList.allshuffleServerSize.DECOMMISSIONING ?? 0 }}
            </div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/decommissionedNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Decommissioned</span>
              </div>
            </template>
            <div class="decommissionednode">
              {{ dataList.allshuffleServerSize.DECOMMISSIONED ?? 0 }}
            </div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/lostNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Lost</span>
              </div>
            </template>
            <div class="lostnode">{{ dataList.allshuffleServerSize.LOST ?? 0 }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/unhealthyNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Unhealthy</span>
              </div>
            </template>
            <div class="unhealthynode">{{ dataList.allshuffleServerSize.UNHEALTHY ?? 0 }}</div>
          </el-card>
        </router-link>
      </el-col>
      <el-col :span="4">
        <router-link to="/shuffleserverpage/excludeNodeList">
          <el-card class="box-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="cardtile">Excludes</span>
              </div>
            </template>
            <div class="excludesnode">{{ dataList.allshuffleServerSize.EXCLUDED ?? 0 }}</div>
          </el-card>
        </router-link>
      </el-col>
    </el-row>
    <el-divider />
    <el-row :gutter="24">
      <div style="width: 100%">
        <router-view></router-view>
      </div>
    </el-row>
  </div>
</template>

<script>
import { onMounted, reactive } from 'vue'
import { getShufflegetStatusTotal } from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

export default {
  setup() {
    const dataList = reactive({
      allshuffleServerSize: {
        ACTIVE: 0,
        DECOMMISSIONED: 0,
        DECOMMISSIONING: 0,
        EXCLUDED: 0,
        LOST: 0,
        UNHEALTHY: 0
      }
    })
    const currentServerStore = useCurrentServerStore()

    async function getShufflegetStatusTotalPage() {
      const res = await getShufflegetStatusTotal()
      dataList.allshuffleServerSize = res.data.data
    }

    // The system obtains data from global variables and requests the interface to obtain new data after data changes.
    currentServerStore.$subscribe((mutable, state) => {
      if (state.currentServer) {
        getShufflegetStatusTotalPage()
      }
    })

    onMounted(() => {
      // If the coordinator address to request is not found in the global variable, the request is not initiated.
      if (currentServerStore.currentServer) {
        getShufflegetStatusTotalPage()
      }
    })
    return { dataList }
  }
}
</script>

<style scoped>
.cardtile {
  font-size: larger;
}

.activenode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: green;
  text-decoration: none;
}

.decommissioningnode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: #00c4ff;
  text-decoration: none;
}

.decommissionednode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: blue;
  text-decoration: none;
}

.lostnode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: red;
  text-decoration: none;
}

.unhealthynode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  color: #ff8800;
  text-decoration: none;
}

.excludesnode {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
  text-decoration: none;
}
</style>
