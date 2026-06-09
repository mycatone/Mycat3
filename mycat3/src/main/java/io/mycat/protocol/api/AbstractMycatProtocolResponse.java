package io.mycat.protocol.api;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.ExecuteType;
import io.mycat.ExplainDetail;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.Response;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlObjectArrayRow;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.mycat.newquery.SqlResult;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Base {@link Response} that drains Mycat's protocol-agnostic SQL pipeline
 * ({@code MycatdbCommand}) into per-front-end-protocol byte writers. Subclasses
 * only have to know how to spell three things on the wire:
 * <ul>
 *   <li>{@link #writeResultSet(MycatRowMetaData, List)} — a SELECT response</li>
 *   <li>{@link #writeOk(long, long, boolean)} — a DML / status response</li>
 *   <li>{@link #writeError(String, int)} — an error response</li>
 * </ul>
 * Everything else (multi-shard fan-out, transaction control, auto-row buffering)
 * is shared with PG / TDS / TNS via this class.
 *
 * <p>This intentionally does NOT implement the Observable&lt;MysqlPayloadObject&gt;
 * path; the MySQL packet-level streaming is MySQL-specific. Non-MySQL protocols
 * buffer rows into a {@code List<Object[]>} per statement, which keeps the
 * implementation tractable at the cost of materializing very large result sets
 * in memory. That cost is acceptable for the PoC; see Mycat3配置说明.md §1.1.
 */
public abstract class AbstractMycatProtocolResponse implements Response {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMycatProtocolResponse.class);

    protected final MycatDataContext dataContext;
    protected final XaSqlConnection transactionSession;
    protected int stmtSize;
    protected int count = 0;

    protected AbstractMycatProtocolResponse(MycatDataContext dataContext, int stmtSize) {
        this.dataContext = dataContext;
        this.transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
        this.stmtSize = stmtSize;
    }

    /** Subclass writes a result set in its wire format. {@code hasMore} hints whether more result sets follow in this batch. */
    protected abstract Future<Void> writeResultSet(MycatRowMetaData metadata, List<Object[]> rows, boolean hasMore);

    /** Subclass writes an OK/CommandComplete/Done packet. {@code hasMore} hints whether more result sets follow. */
    protected abstract Future<Void> writeOk(long affectedRow, long lastInsertId, boolean hasMore);

    /** Subclass writes an error packet (terminal — protocol typically resyncs after this). */
    protected abstract Future<Void> writeError(String message, int code);

    protected boolean hasMoreResultSet() {
        return count < this.stmtSize;
    }

    @Override
    public int getResultSetCounter() {
        return count;
    }

    @Override
    public void resetResultSetSize(int size) {
        this.stmtSize = size;
    }

    @Override
    public Future<Void> sendError(Throwable e) {
        dataContext.setLastMessage(e);
        String msg = e.getMessage() == null ? e.toString() : e.getMessage();
        int code = (e instanceof MycatException) ? ((MycatException) e).getErrorCode() : 1064;
        return writeError(msg, code);
    }

    @Override
    public Future<Void> sendError(String errorMessage, int errorCode) {
        dataContext.setLastMessage(errorMessage);
        dataContext.setLastErrorCode(errorCode);
        return writeError(errorMessage, errorCode);
    }

    @Override
    public Future<Void> proxySelect(List<String> targets, String statement, List<Object> params) {
        return execute(ExplainDetail.create(ExecuteType.QUERY, targets, statement, null, params));
    }

    @Override
    public Future<Void> proxyInsert(List<String> targets, String proxyUpdate, List<Object> params) {
        return execute(ExplainDetail.create(ExecuteType.INSERT, targets, proxyUpdate, null, params));
    }

    @Override
    public Future<Void> proxyUpdate(List<String> targets, String proxyUpdate, List<Object> params) {
        return execute(ExplainDetail.create(ExecuteType.UPDATE, targets, proxyUpdate, null, params));
    }

    @Override
    public Future<Void> proxyUpdateToPrototype(String proxyUpdate, List<Object> params) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return proxyUpdate(Collections.singletonList(metadataManager.getPrototype()), proxyUpdate, params);
    }

    @Override
    public Future<Void> proxySelectToPrototype(String statement, List<Object> params) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return execute(ExplainDetail.create(ExecuteType.QUERY_MASTER,
                Collections.singletonList(metadataManager.getPrototype()), statement, null, params));
    }

    @Override
    public Future<Void> execute(ExplainDetail detail) {
        boolean master = dataContext.isInTransaction()
                || !dataContext.isAutocommit()
                || detail.getExecuteType().isMaster();
        Set<String> targets = new LinkedHashSet<>();
        for (String target : detail.getTargets()) {
            targets.add(dataContext.resolveDatasourceTargetName(target, master));
        }
        List<String> targetList = new ArrayList<>(targets);
        String sql = detail.getSql();
        List<Object> params = detail.getParams() == null ? Collections.emptyList() : detail.getParams();

        switch (detail.getExecuteType()) {
            case QUERY:
            case QUERY_MASTER: {
                if (targetList.isEmpty()) {
                    count++;
                    return writeResultSet(EmptyMeta.INSTANCE, Collections.emptyList(), hasMoreResultSet());
                }
                List<Future<RowSet>> rowSetFutures = new ArrayList<>(targetList.size());
                for (String ds : targetList) {
                    Future<NewMycatConnection> connFuture = transactionSession.getConnection(ds);
                    rowSetFutures.add(connFuture.flatMap(c -> c.query(sql, params)));
                }
                @SuppressWarnings({"unchecked", "rawtypes"})
                CompositeFuture all = CompositeFuture.all((List) rowSetFutures);
                return all.flatMap(joined -> {
                    List<RowSet> rowSets = all.list();
                    MycatRowMetaData metadata = rowSets.isEmpty()
                            ? EmptyMeta.INSTANCE
                            : rowSets.get(0).getMycatRowMetaData();
                    List<Object[]> merged = new ArrayList<>();
                    for (RowSet rs : rowSets) {
                        for (Object[] row : rs) merged.add(row);
                    }
                    count++;
                    boolean hasMore = hasMoreResultSet();
                    return transactionSession.closeStatementState()
                            .flatMap(u -> writeResultSet(metadata, merged, hasMore));
                }).recover(t -> {
                    LOGGER.error("execute QUERY failed: {}", sql, t);
                    return sendError(t);
                });
            }
            case INSERT:
            case UPDATE: {
                if (targetList.isEmpty()) {
                    count++;
                    return writeOk(0, 0, hasMoreResultSet());
                }
                List<Future<SqlResult>> updates = new ArrayList<>(targetList.size());
                for (String ds : targetList) {
                    Future<NewMycatConnection> connFuture = transactionSession.getConnection(ds);
                    if (detail.getExecuteType() == ExecuteType.INSERT) {
                        updates.add(connFuture.flatMap(c -> c.insert(sql, params)));
                    } else {
                        updates.add(connFuture.flatMap(c -> c.update(sql, params)));
                    }
                }
                @SuppressWarnings({"unchecked", "rawtypes"})
                CompositeFuture all = CompositeFuture.all((List) updates);
                return all.flatMap(joined -> {
                    long affected = 0;
                    long lastInsertId = 0;
                    for (Object r : all.list()) {
                        SqlResult sr = (SqlResult) r;
                        affected += sr.getAffectRows();
                        if (sr.getLastInsertId() > lastInsertId) lastInsertId = sr.getLastInsertId();
                    }
                    dataContext.setAffectedRows(affected);
                    dataContext.setLastInsertId(lastInsertId);
                    count++;
                    long finalAffected = affected;
                    long finalLastId = lastInsertId;
                    return transactionSession.closeStatementState()
                            .flatMap(u -> writeOk(finalAffected, finalLastId, hasMoreResultSet()));
                }).recover(t -> {
                    LOGGER.error("execute DML failed: {}", sql, t);
                    return sendError(t);
                });
            }
            default:
                return sendError("Unexpected ExecuteType: " + detail.getExecuteType(), 1064);
        }
    }

    /**
     * Override of the Observable-based sendResultSet variant. The default in
     * {@link Response} throws {@link UnsupportedOperationException}, but Mycat's
     * Calcite pipeline ({@code ObservablePlanImplementorImpl#executeQuery}) emits
     * results through this overload — not via {@link #sendResultSet(RowBaseIterator)}.
     * Without this override, every SELECT routed through Calcite explodes for
     * PG / TDS / TNS front-ends.
     *
     * Buffer-first model: collect the Observable into ({@link MycatRowMetaData}, rows)
     * then call the protocol-specific {@link #writeResultSet}. Acceptable for PoC;
     * a true streaming write would back-pressure to the protocol layer.
     */
    @Override
    public Future<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
        io.vertx.core.Promise<Void> promise = io.vertx.core.Promise.promise();
        final MycatRowMetaData[] metaHolder = new MycatRowMetaData[1];
        final List<Object[]> rows = new ArrayList<>();
        mysqlPacketObservable.subscribe(
                payload -> {
                    if (payload instanceof MySQLColumnDef) {
                        metaHolder[0] = ((MySQLColumnDef) payload).getMetaData();
                    } else if (payload instanceof MysqlObjectArrayRow) {
                        rows.add(((MysqlObjectArrayRow) payload).getRow());
                    }
                    // ignore other MysqlPayloadObject subtypes (e.g. binary payload rows)
                },
                err -> {
                    LOGGER.error("sendResultSet observable failed", err);
                    sendError((Throwable) err).onComplete(ar -> promise.tryFail((Throwable) err));
                },
                () -> {
                    count++;
                    MycatRowMetaData meta = metaHolder[0] != null ? metaHolder[0] : EmptyMeta.INSTANCE;
                    boolean hasMore = hasMoreResultSet();
                    writeResultSet(meta, rows, hasMore).onComplete(ar -> {
                        if (ar.succeeded()) promise.tryComplete();
                        else promise.tryFail(ar.cause());
                    });
                });
        return promise.future();
    }

    @Override
    public Future<Void> sendResultSet(RowBaseIterator rowBaseIterator) {
        try {
            MycatRowMetaData metadata = rowBaseIterator.getMetaData();
            List<Object[]> rows = new ArrayList<>();
            while (rowBaseIterator.next()) {
                rows.add(rowBaseIterator.getObjects());
            }
            count++;
            return writeResultSet(metadata, rows, hasMoreResultSet());
        } finally {
            try { rowBaseIterator.close(); } catch (Exception ignored) {}
        }
    }

    @Override
    public Future<Void> sendOk() {
        count++;
        return transactionSession.closeStatementState()
                .flatMap(u -> writeOk(0, 0, hasMoreResultSet()));
    }

    @Override
    public Future<Void> sendOk(long affectedRow) {
        dataContext.setAffectedRows(affectedRow);
        count++;
        return transactionSession.closeStatementState()
                .flatMap(u -> writeOk(affectedRow, 0, hasMoreResultSet()));
    }

    @Override
    public Future<Void> sendOk(long affectedRow, long lastInsertId) {
        dataContext.setAffectedRows(affectedRow);
        dataContext.setLastInsertId(lastInsertId);
        count++;
        return transactionSession.closeStatementState()
                .flatMap(u -> writeOk(affectedRow, lastInsertId, hasMoreResultSet()));
    }

    @Override
    public Future<Void> begin() {
        count++;
        return transactionSession.begin()
                .flatMap(u -> writeOk(0, 0, hasMoreResultSet()));
    }

    @Override
    public Future<Void> commit() {
        count++;
        return transactionSession.commit()
                .flatMap(u -> writeOk(0, 0, hasMoreResultSet()));
    }

    @Override
    public Future<Void> rollback() {
        count++;
        return transactionSession.rollback()
                .flatMap(u -> writeOk(0, 0, hasMoreResultSet()));
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }

    @Override
    public Future<Void> swapBuffer(Observable<Buffer> sender) {
        return Future.failedFuture(new UnsupportedOperationException(
                "swapBuffer is MySQL-protocol-only; not available on PG/TDS/TNS front-ends"));
    }

    @Override
    public Future<Void> sendVectorResultSet(MycatRowMetaData metaData,
                                            Observable<VectorSchemaRoot> observable) {
        return Future.failedFuture(new UnsupportedOperationException(
                "vector result set not supported on PG/TDS/TNS front-ends"));
    }

    @Override
    public Future<Void> proxyProcedure(String sql, String targetName) {
        return sendError("CALL/procedure not supported on PG/TDS/TNS front-ends", 1064);
    }

    @Override
    public Future<Void> rollbackSavepoint(String name) {
        return sendError("savepoint not supported on PG/TDS/TNS front-ends", 1064);
    }

    @Override
    public Future<Void> setSavepoint(String name) {
        return sendError("savepoint not supported on PG/TDS/TNS front-ends", 1064);
    }

    @Override
    public Future<Void> releaseSavepoint(String name) {
        return sendError("savepoint not supported on PG/TDS/TNS front-ends", 1064);
    }

    private enum EmptyMeta implements MycatRowMetaData {
        INSTANCE;

        @Override public int getColumnCount() { return 0; }
        @Override public boolean isAutoIncrement(int column) { return false; }
        @Override public boolean isCaseSensitive(int column) { return false; }
        @Override public boolean isSigned(int column) { return false; }
        @Override public int getColumnDisplaySize(int column) { return 0; }
        @Override public String getColumnName(int column) { return ""; }
        @Override public String getSchemaName(int column) { return ""; }
        @Override public int getPrecision(int column) { return 0; }
        @Override public int getScale(int column) { return 0; }
        @Override public String getTableName(int column) { return ""; }
        @Override public int getColumnType(int column) { return java.sql.Types.NULL; }
        @Override public String getColumnLabel(int column) { return ""; }
        @Override public java.sql.ResultSetMetaData metaData() { return null; }
        @Override public boolean isNullable(int column) { return true; }
    }
}
