package com.aliyun.polardbx.demo.class3;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.clickhouse.jdbc.ClickHouseDataSource;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 开源学堂《跟我学 PolarDB-X》之三
 * 《如何将 PolarDB-X 与大数据等系统互通》
 *
 * @author 燧木
 * @date 2022.01.14
 */
public class XCanalCHDemo {
    public static void main(String[] args) {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(AddressUtils.getHostIp(), 11111),
                "test",
                "",
                "");

        int batchSize = 1000;
        int emptyCount = 0;

        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmptyCount = 120;
            while (emptyCount < totalEmptyCount) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    printEntry(message.getEntries());
                }

                // 提交确认
                connector.ack(batchId);
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<Entry> entrys) {
        String url = "jdbc:ch://localhost/testdb";
        Properties properties = new Properties();
        // optionally set connection properties
        properties.setProperty("client_name", "Agent #1");

        ClickHouseDataSource dataSource = null;
        Statement statement = null;
        try {
            dataSource = new ClickHouseDataSource(url, properties);
            Connection conn = dataSource.getConnection();
            statement = conn.createStatement();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.INSERT) {
                    printColumn(statement, rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(Statement statement, List<Column> columns) {
        Map<String, Column> nameColumnMap = new HashMap<>(2);
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            nameColumnMap.put(column.getName(), column);
        }
        String sql = "INSERT INTO test(id, name) values(" + nameColumnMap.get("id").getValue() + ",'" + nameColumnMap.get("name").getValue() + "')";
        try {
            statement.executeUpdate(sql);
            System.out.println("SQL done: " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
