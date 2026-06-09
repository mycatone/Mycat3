package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLParameter;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.*;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.config.NormalBackEndProcedureInfoConfig;
import io.mycat.config.NormalProcedureConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

public class SQLCreateProcedureHandler extends AbstractSQLHandler<SQLCreateProcedureStatement> {


    @Override
    @SneakyThrows
    protected Future<Void> onExecute(SQLRequest<SQLCreateProcedureStatement> request, MycatDataContext dataContext, Response response) {

        SQLCreateProcedureStatement ast = request.getAst();
        if (ast.getName() instanceof SQLIdentifierExpr) {
            String defaultSchema = dataContext.getDefaultSchema();
            if (defaultSchema != null) {
                ast.setName(new SQLPropertyExpr( defaultSchema , ((SQLIdentifierExpr) ast.getName()).getName()));
            }
        }

        if (!(ast.getName() instanceof SQLPropertyExpr)) {
            throw new IllegalArgumentException("unknown schema:");
        }
        SQLPropertyExpr pNameExpr = (SQLPropertyExpr) ast.getName();
        String schemaName = SQLUtils.normalize(pNameExpr.getOwnerName().toLowerCase());
        String pName = SQLUtils.normalize(pNameExpr.getName().toLowerCase());
        List<SQLParameter> sqlParameters = Optional.ofNullable(ast.getParameters()).orElse(Collections.emptyList());
        Map<SQLParameter.ParameterType, List<SQLParameter>> parameterTypeListMap
                = sqlParameters.stream().collect(Collectors.groupingBy(k -> k.getParamType()));
        SQLBlockStatement block = (SQLBlockStatement)ast.getBlock();
        if (dataContext.getDefaultSchema() != null) {
            block.accept(new MySqlASTVisitorAdapter() {
                @Override
                public void endVisit(SQLExprTableSource x) {
                    resolveSQLExprTableSource(x, dataContext);
                }
            });
        }
        Map<String, Collection<String>> collect = TableCollector.collect(dataContext.getDefaultSchema(), block);


        int resultSetCount = getResultSetCount(block);

        List<TableHandler> tableHandlers = getTableHandlers(block);
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        NormalProcedureConfig normalProcedureConfig = new NormalProcedureConfig();
        normalProcedureConfig.setCreateProcedureSQL(ast.toString());

        NormalBackEndProcedureInfoConfig normalBackEndProcedureInfoConfig = new NormalBackEndProcedureInfoConfig();
        normalBackEndProcedureInfoConfig.setProcedureName(pName);
        normalBackEndProcedureInfoConfig.setSchemaName(schemaName);
        normalBackEndProcedureInfoConfig.setTargetName(MetadataManager.getPrototype());


        normalProcedureConfig.setLocality(normalBackEndProcedureInfoConfig);

        try(MycatRouterConfigOps ops = ConfigUpdater.getOps();){
            ops.addProcedure(schemaName,pName,normalProcedureConfig);
            ops.commit();
        }

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);

        try(DefaultConnection connection = jdbcConnectionManager.getConnection(MetadataManager.getPrototype());){
            connection.executeUpdate(ast.toString(),false);
        }
        return response.sendOk();
    }

    @NotNull
    private List<TableHandler> getTableHandlers(SQLBlockStatement block) {
        List<TableHandler> tableHandlers = new ArrayList<>();

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        for (SQLStatement sqlStatement : block.getStatementList()) {
            sqlStatement.accept(new MySqlASTVisitorAdapter(){
                @Override
                public void endVisit(SQLExprTableSource x) {
                    String schemaName = SQLUtils.normalize(x.getSchema());
                    String tableName =  SQLUtils.normalize(x.getTableName());
                    tableHandlers.add(metadataManager.getTable(schemaName,tableName));
                }
            });
        }
        return tableHandlers;
    }

    private int getResultSetCount(SQLBlockStatement block) {
        int resultSetCount = 0;
        List<SQLStatement> statementList = block.getStatementList();
        for (SQLStatement sqlStatement : statementList) {
            if(sqlStatement instanceof SQLSelectStatement){
                resultSetCount++;
            }
        }
        return resultSetCount;
    }
}
