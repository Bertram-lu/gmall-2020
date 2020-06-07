package com.atguigu.utils

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

import collection.JavaConversions._

object MyEsUtil {

    private val ES_HOST = "http://hadoop102"
    private val ES_HTTP_PORT = 9200
    private var factory: JestClientFactory = _

    /**
      * 获取客户端
      *
      * @return jestclient
      */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
      * 关闭客户端
      */
    def close(client: JestClient): Unit = {
        if (Objects.isNull(client))
            client.shutdownClient()
    }

    /**
      * 建立连接
      */
    def build() = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
            .multiThreaded(true)
            .maxTotalConnection(200)
            .connTimeout(10000)
            .readTimeout(10000)
            .build()
        )
    }

    // 批量插入数据到ES
    def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {

        if (docList.nonEmpty) {

            //获取ES客户端连接
            val jest: JestClient = getClient

            //创建Bulk.Builder对象
            val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")

            //遍历docList,创建Index存放至Bulk.Builder中
            for ((id, doc) <- docList) {
                val indexBuilder = new Index.Builder(doc)
                if (id != null) {
                    indexBuilder.id(id)
                }
                val index: Index = indexBuilder.build()
                bulkBuilder.addAction(index)
            }

            //创建Bulk对象
            val bulk: Bulk = bulkBuilder.build()

            var items: util.List[BulkResult#BulkResultItem] = null
            try {

                //执行写入数据操作
                items = jest.execute(bulkBuilder.build()).getItems
            } catch {
                case ex: Exception => println(ex.toString)
            } finally {
                close(jest)
                println("保存" + items.size() + "条数据")
                for (item <- items) {
                    if (item.error != null && item.error.nonEmpty) {
                        println(item.error)
                        println(item.errorReason)
                    }
                }
            }
        }
    }
}
