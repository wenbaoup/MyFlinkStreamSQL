/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.yjp.flink.sql.side;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.yjp.flink.sql.config.CalciteConfig;
import com.yjp.flink.sql.util.ParseUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * Parsing sql, obtain execution information dimension table
 * Date: 2018/7/24
 * Company: www.yjp.com
 *
 * @author xuchao
 */

public class SideSQLParser {

    private static final Logger LOG = LoggerFactory.getLogger(SideSQLParser.class);

    private final char SPLIT = '_';

    private String tempSQL = "SELECT * FROM TMP";

    public Queue<Object> getExeQueue(String exeSql, Set<String> sideTableSet) throws SqlParseException {
        System.out.println("---exeSql---");
        System.out.println(exeSql);
        LOG.info("---exeSql---");
        LOG.info(exeSql);

        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        //用mysql语法 解析sql
        SqlParser sqlParser = SqlParser.create(exeSql, CalciteConfig.MYSQL_LEX_CONFIG);
        SqlNode sqlNode = sqlParser.parseStmt();
        parseSql(sqlNode, sideTableSet, queueInfo);
        queueInfo.offer(sqlNode);
        return queueInfo;
    }

    private Object parseSql(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case WITH: {
                SqlWith sqlWith = (SqlWith) sqlNode;
                SqlNodeList sqlNodeList = sqlWith.withList;
                for (SqlNode withAsTable : sqlNodeList) {
                    SqlWithItem sqlWithItem = (SqlWithItem) withAsTable;
                    parseSql(sqlWithItem.query, sideTableSet, queueInfo);
                    queueInfo.add(sqlWithItem);
                }
                parseSql(sqlWith.body, sideTableSet, queueInfo);
                break;
            }
            case INSERT:
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                return parseSql(sqlSource, sideTableSet, queueInfo);
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect) sqlNode).getFrom();
                if (sqlFrom.getKind() != IDENTIFIER) {
                    Object result = parseSql(sqlFrom, sideTableSet, queueInfo);
                    if (result instanceof JoinInfo) {
                        dealSelectResultWithJoinInfo((JoinInfo) result, (SqlSelect) sqlNode, queueInfo);
                    } else if (result instanceof AliasInfo) {
                        String tableName = ((AliasInfo) result).getName();
                        if (sideTableSet.contains(tableName)) {
                            throw new RuntimeException("side-table must be used in join operator");
                        }
                    }
                } else {
                    String tableName = ((SqlIdentifier) sqlFrom).getSimple();
                    if (sideTableSet.contains(tableName)) {
                        throw new RuntimeException("side-table must be used in join operator");
                    }
                }
                break;
            case JOIN:
                return dealJoinNode((SqlJoin) sqlNode, sideTableSet, queueInfo);
            case AS:
                SqlNode info = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];
                String infoStr;

                if (info.getKind() == IDENTIFIER) {
                    infoStr = info.toString();
                } else {
                    infoStr = parseSql(info, sideTableSet, queueInfo).toString();
                }

                AliasInfo aliasInfo = new AliasInfo();
                aliasInfo.setName(infoStr);
                aliasInfo.setAlias(alias.toString());

                return aliasInfo;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];

                parseSql(unionLeft, sideTableSet, queueInfo);

                parseSql(unionRight, sideTableSet, queueInfo);

                break;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
                parseSql(sqlOrderBy.query, sideTableSet, queueInfo);
            default:
        }

        return "";
    }

    private JoinInfo dealJoinNode(SqlJoin joinNode, Set<String> sideTableSet, Queue<Object> queueInfo) {
        SqlNode leftNode = joinNode.getLeft();
        SqlNode rightNode = joinNode.getRight();
        JoinType joinType = joinNode.getJoinType();
        String leftTbName = "";
        String leftTbAlias = "";
        String rightTableName = "";
        String rightTableAlias = "";
        Map<String, String> midTableMapping = null;
        boolean leftIsMidTable = false;//判断左边是不是中间表
        // 右节点已经被解析 判断右边是不是多层join
        boolean rightIsParse = false;
        Tuple2<String, String> rightTableNameAndAlias = null;


        if (leftNode.getKind() == IDENTIFIER) {
            leftTbName = leftNode.toString();
        } else if (leftNode.getKind() == JOIN) {
            //解析多JOIN
            JoinInfo leftNodeJoinInfo = (JoinInfo) parseSql(leftNode, sideTableSet, queueInfo);
            rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo);
            rightIsParse = true;

            if (checkIsSideTable(rightTableNameAndAlias.f0, sideTableSet)) {
                //  select * from xxx
                SqlNode sqlNode = buildSelectByLeftNode(leftNode);
                //  ( select * from xxx) as xxx_0
                SqlBasicCall newAsNode = buildAsNodeByJoinInfo(leftNodeJoinInfo, sqlNode);
                leftNode = newAsNode;
                joinNode.setLeft(leftNode);

                leftIsMidTable = true;
                midTableMapping = saveTabMapping(leftNodeJoinInfo);

                AliasInfo aliasInfo = (AliasInfo) parseSql(newAsNode, sideTableSet, queueInfo);
                leftTbName = aliasInfo.getName();
                leftTbAlias = aliasInfo.getAlias();
            } else {
                leftTbName = leftNodeJoinInfo.getRightTableName();
                leftTbAlias = leftNodeJoinInfo.getRightTableAlias();
            }

        } else if (leftNode.getKind() == AS) {
            AliasInfo aliasInfo = (AliasInfo) parseSql(leftNode, sideTableSet, queueInfo);
            leftTbName = aliasInfo.getName();
            leftTbAlias = aliasInfo.getAlias();
        } else {
            throw new RuntimeException("---not deal---");
        }

        boolean leftIsSide = checkIsSideTable(leftTbName, sideTableSet);
        if (leftIsSide) {
            throw new RuntimeException("side-table must be at the right of join operator");
        }


        if (!rightIsParse) {
            rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo);
        }
        rightTableName = rightTableNameAndAlias.f0;
        rightTableAlias = rightTableNameAndAlias.f1;


        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);
        if (joinType == JoinType.RIGHT) {
            throw new RuntimeException("side join not support join type of right[current support inner join and left join]");
        }

        if (leftIsMidTable) {
            //  替换右边 on语句 中的字段别名
            SqlNode afterReplaceNameCondition = ParseUtils.replaceJoinConditionTabName(joinNode.getCondition(), midTableMapping);
            joinNode.setOperand(5, afterReplaceNameCondition);
        }

        JoinInfo tableInfo = new JoinInfo();
        tableInfo.setLeftTableName(leftTbName);
        tableInfo.setRightTableName(rightTableName);
        if ("".equals(leftTbAlias)) {
            tableInfo.setLeftTableAlias(leftTbName);
        } else {
            tableInfo.setLeftTableAlias(leftTbAlias);
        }
        if ("".equals(leftTbAlias)) {
            tableInfo.setRightTableAlias(rightTableName);
        } else {
            tableInfo.setRightTableAlias(rightTableAlias);
        }
        //如果leftIsSide==true 前面已经抛出异常
        tableInfo.setLeftIsSideTable(false);
        tableInfo.setRightIsSideTable(rightIsSide);
        tableInfo.setLeftNode(leftNode);
        tableInfo.setRightNode(rightNode);
        tableInfo.setJoinType(joinType);
        tableInfo.setCondition(joinNode.getCondition());

        tableInfo.setLeftIsMidTable(leftIsMidTable);
        tableInfo.setLeftTabMapping(midTableMapping);

        return tableInfo;
    }


    private Tuple2<String, String> parseRightNode(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo) {
        Tuple2<String, String> tabName = new Tuple2<>("", "");
        if (sqlNode.getKind() == IDENTIFIER) {
            tabName.f0 = sqlNode.toString();
        } else {
            AliasInfo aliasInfo = (AliasInfo) parseSql(sqlNode, sideTableSet, queueInfo);
            tabName.f0 = aliasInfo.getName();
            tabName.f1 = aliasInfo.getAlias();
        }
        return tabName;
    }

    /**
     * 层层解析
     * 将第二层left join的左节点解析为 select * from A left join B
     * 第三层left join的左节点解析为  select * from (select * from A left join B) as c left join D
     *
     * @param leftNode
     * @return
     */
    private SqlNode buildSelectByLeftNode(SqlNode leftNode) {
        SqlParser sqlParser = SqlParser.create(tempSQL, CalciteConfig.MYSQL_LEX_CONFIG);
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (Exception e) {
            LOG.error("tmp sql parse error..", e);
        }

        assert sqlNode != null;
        ((SqlSelect) sqlNode).setFrom(leftNode);
        return sqlNode;
    }


    private Map<String, String> saveTabMapping(JoinInfo leftNodeJoinInfo) {
        Map<String, String> midTableMapping = Maps.newHashMap();

        String midTab = buidTableName(leftNodeJoinInfo.getLeftTableAlias(), SPLIT, leftNodeJoinInfo.getRightTableAlias());
        String finalMidTab = midTab + "_0";

        if (leftNodeJoinInfo.isLeftIsMidTable()) {
            midTableMapping.putAll(leftNodeJoinInfo.getLeftTabMapping());
        }
        fillLeftAllTable(leftNodeJoinInfo, midTableMapping, finalMidTab);
        return midTableMapping;
    }

    private void fillLeftAllTable(JoinInfo leftNodeJoinInfo, Map<String, String> midTableMapping, String finalMidTab) {
        List<String> tablesName = Lists.newArrayList();
        ParseUtils.parseLeftNodeTableName(leftNodeJoinInfo.getLeftNode(), tablesName);

        tablesName.forEach(tab -> {
            midTableMapping.put(tab, finalMidTab);
        });
        midTableMapping.put(leftNodeJoinInfo.getRightTableAlias(), finalMidTab);
    }

    private void dealSelectResultWithJoinInfo(JoinInfo joinInfo, SqlSelect sqlNode, Queue<Object> queueInfo) {
        //  中间虚拟表进行表名称替换
        if (joinInfo.isLeftIsMidTable()) {
            SqlNode whereNode = sqlNode.getWhere();
            SqlNodeList sqlGroup = sqlNode.getGroup();
            SqlNodeList sqlSelectList = sqlNode.getSelectList();
            List<SqlNode> newSelectNodeList = Lists.newArrayList();

            for (int i = 0; i < sqlSelectList.getList().size(); i++) {
                SqlNode selectNode = sqlSelectList.getList().get(i);

                SqlNode replaceNode = ParseUtils.replaceSelectFieldTabName(selectNode, joinInfo.getLeftTabMapping());

                if (replaceNode == null) {
                    continue;
                }
                //sqlSelectList.set(i, replaceNode);
                newSelectNodeList.add(replaceNode);
            }

            SqlNodeList newSelectList = new SqlNodeList(newSelectNodeList, sqlSelectList.getParserPosition());
            sqlNode.setSelectList(newSelectList);


            //where
            if (whereNode != null) {
                SqlNode[] sqlNodeList = ((SqlBasicCall) whereNode).getOperands();
                for (int i = 0; i < sqlNodeList.length; i++) {
                    SqlNode whereSqlNode = sqlNodeList[i];
                    SqlNode replaceNode = ParseUtils.replaceNodeInfo(whereSqlNode, joinInfo.getLeftTabMapping());
                    sqlNodeList[i] = replaceNode;
                }
            }

            if (sqlGroup != null && CollectionUtils.isNotEmpty(sqlGroup.getList())) {
                for (int i = 0; i < sqlGroup.getList().size(); i++) {
                    SqlNode selectNode = sqlGroup.getList().get(i);
                    SqlNode replaceNode = ParseUtils.replaceNodeInfo(selectNode, joinInfo.getLeftTabMapping());
                    sqlGroup.set(i, replaceNode);
                }
            }
        }

        //SideJoinInfo rename
        if (joinInfo.checkIsSide()) {
            joinInfo.setSelectFields(sqlNode.getSelectList());
            joinInfo.setSelectNode(sqlNode);
            if (joinInfo.isRightIsSideTable()) {
                //Analyzing left is not a simple table
                if (joinInfo.getLeftNode().toString().contains("SELECT")) {
                    queueInfo.offer(joinInfo.getLeftNode());
                }

                queueInfo.offer(joinInfo);
            } else {
                //Determining right is not a simple table
                if (joinInfo.getRightNode().getKind() == SELECT) {
                    queueInfo.offer(joinInfo.getLeftNode());
                }

                queueInfo.offer(joinInfo);
            }
            replaceFromNodeForJoin(joinInfo, sqlNode);

        }
    }


    private void replaceFromNodeForJoin(JoinInfo joinInfo, SqlSelect sqlNode) {
        //Update from node
        SqlBasicCall sqlBasicCall = buildAsNodeByJoinInfo(joinInfo, null);
        sqlNode.setFrom(sqlBasicCall);
    }


    private SqlBasicCall buildAsNodeByJoinInfo(JoinInfo joinInfo, SqlNode sqlNode0) {
        SqlOperator operator = new SqlAsOperator();

        SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
        String joinLeftTableName = joinInfo.getLeftTableName();
        String joinLeftTableAlias = joinInfo.getLeftTableAlias();
        joinLeftTableName = Strings.isNullOrEmpty(joinLeftTableName) ? joinLeftTableAlias : joinLeftTableName;
        String newTableName = buidTableName(joinLeftTableName, SPLIT, joinInfo.getRightTableName());
        String newTableAlias = buidTableName(joinInfo.getLeftTableAlias(), SPLIT, joinInfo.getRightTableAlias());

        //  mid table alias  a_b_0
        if (null != sqlNode0) {
            newTableAlias += "_0";
        }

        if (null == sqlNode0) {
            sqlNode0 = new SqlIdentifier(newTableName, null, sqlParserPos);
        }

        SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(newTableAlias, null, sqlParserPos);
        SqlNode[] sqlNodes = new SqlNode[2];
        sqlNodes[0] = sqlNode0;
        sqlNodes[1] = sqlIdentifierAlias;
        return new SqlBasicCall(operator, sqlNodes, sqlParserPos);
    }

    private String buidTableName(String left, char split, String right) {
        return left + split + right;
    }

    private boolean checkIsSideTable(String tableName, Set<String> sideTableList) {
        return sideTableList.contains(tableName);

    }
}
