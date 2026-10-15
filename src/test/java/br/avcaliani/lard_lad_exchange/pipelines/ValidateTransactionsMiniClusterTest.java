package br.avcaliani.lard_lad_exchange.pipelines;

import br.avcaliani.lard_lad_exchange.models.out.DTOTransaction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static br.avcaliani.lard_lad_exchange.pipelines.Fixtures.dto;
import static br.avcaliani.lard_lad_exchange.pipelines.Fixtures.user;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs the validation step inside a real (mini) Flink cluster,
 * so the function and the records also go through Flink serialization.
 */
class ValidateTransactionsMiniClusterTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build()
    );

    @Test
    void validatesTransactionsEndToEnd() throws Exception {
        var homer = user("1", "Homer");
        var marge = user("2", "Marge");
        var input = List.of(
                dto("ok", homer, marge),
                dto("missing-user", null, marge),
                dto("same-user", homer, homer)
        );

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var result = new ArrayList<DTOTransaction>();
        try (var rows = env.fromData(input)
                .map(ValidateTransactions::isTxnValid)
                .executeAndCollect()) {
            rows.forEachRemaining(result::add);
        }

        Map<String, Boolean> validById = result.stream()
                .collect(Collectors.toMap(DTOTransaction::getId, DTOTransaction::isValid));
        assertEquals(Map.of("ok", true, "missing-user", false, "same-user", false), validById);
    }
}
