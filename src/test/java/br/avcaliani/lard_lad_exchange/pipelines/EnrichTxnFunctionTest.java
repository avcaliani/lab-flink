package br.avcaliani.lard_lad_exchange.pipelines;

import br.avcaliani.lard_lad_exchange.models.in.Transaction;
import br.avcaliani.lard_lad_exchange.models.in.User;
import br.avcaliani.lard_lad_exchange.models.out.DTOTransaction;
import br.avcaliani.lard_lad_exchange.pipelines.ValidateTransactions.EnrichTxnFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static br.avcaliani.lard_lad_exchange.pipelines.Fixtures.txn;
import static br.avcaliani.lard_lad_exchange.pipelines.Fixtures.user;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Operator test harness: it feeds the broadcast function directly, no cluster needed.
 */
class EnrichTxnFunctionTest {

    private final MapStateDescriptor<String, User> descriptor =
            new MapStateDescriptor<>("userBroadcastState", String.class, User.class);

    private BroadcastOperatorTestHarness<Transaction, User, DTOTransaction> harness() throws Exception {
        var operator = new CoBroadcastWithNonKeyedOperator<>(new EnrichTxnFunction(descriptor), List.of(descriptor));
        var harness = new BroadcastOperatorTestHarness<>(operator, 1, 1, 0);
        harness.setup();
        harness.open();
        return harness;
    }

    @Test
    void enrichesTransactionWithSenderAndReceiver() throws Exception {
        try (var harness = harness()) {
            harness.processBroadcastElement(user("1", "Homer"), 1L);
            harness.processBroadcastElement(user("2", "Marge"), 1L);
            harness.processElement(txn("t1", "1", "2"), 2L);

            var out = harness.extractOutputValues();
            assertEquals(1, out.size());
            assertEquals("t1", out.get(0).getId());
            assertEquals("Homer", out.get(0).getSender().getName());
            assertEquals("Marge", out.get(0).getReceiver().getName());
        }
    }

    @Test
    void leavesUserNullWhenItIsNotInTheBroadcastState() throws Exception {
        try (var harness = harness()) {
            harness.processBroadcastElement(user("1", "Homer"), 1L);
            harness.processElement(txn("t1", "1", "999"), 2L);

            var out = harness.extractOutputValues();
            assertEquals(1, out.size());
            assertEquals("Homer", out.get(0).getSender().getName());
            assertNull(out.get(0).getReceiver());
        }
    }

    @Test
    void keepsTheTransactionIdsForReporting() throws Exception {
        try (var harness = harness()) {
            harness.processElement(txn("t1", "1", "2"), 1L);

            var out = harness.extractOutputValues();
            assertEquals("1", out.get(0).getFrom());
            assertEquals("2", out.get(0).getTo());
        }
    }

    @Test
    void lastBroadcastUserWins() throws Exception {
        try (var harness = harness()) {
            harness.processBroadcastElement(user("1", "Homer"), 1L);
            harness.processBroadcastElement(user("1", "Homer J. Simpson"), 2L);

            var state = harness.getBroadcastState(descriptor);
            assertEquals("Homer J. Simpson", state.get("1").getName());
        }
    }
}
