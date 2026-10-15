package br.avcaliani.lard_lad_exchange.pipelines;

import org.junit.jupiter.api.Test;

import static br.avcaliani.lard_lad_exchange.pipelines.Fixtures.dto;
import static br.avcaliani.lard_lad_exchange.pipelines.Fixtures.user;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidateTransactionsTest {

    private static final String MISSING_USER = "missing user(s)";
    private static final String SAME_USER = "same user as sender and receiver";

    @Test
    void validWhenSenderAndReceiverAreDifferentUsers() {
        var txn = ValidateTransactions.isTxnValid(dto("t1", user("1", "Homer"), user("2", "Marge")));

        assertTrue(txn.isValid());
        assertTrue(txn.getErrors().isEmpty());
    }

    @Test
    void invalidWhenSenderIsMissing() {
        var txn = ValidateTransactions.isTxnValid(dto("t1", null, user("2", "Marge")));

        assertTrue(txn.isInvalid());
        assertEquals(1, txn.getErrors().size());
        assertTrue(txn.getErrors().get(0).contains(MISSING_USER));
    }

    @Test
    void invalidWhenReceiverIsMissing() {
        var txn = ValidateTransactions.isTxnValid(dto("t1", user("1", "Homer"), null));

        assertTrue(txn.isInvalid());
        assertTrue(txn.getErrors().get(0).contains(MISSING_USER));
    }

    @Test
    void invalidWhenBothUsersAreMissing() {
        var txn = ValidateTransactions.isTxnValid(dto("t1", null, null));

        assertTrue(txn.isInvalid());
        assertFalse(txn.getErrors().isEmpty());
    }

    @Test
    void invalidWhenSenderAndReceiverAreTheSameUser() {
        var homer = user("1", "Homer");
        var txn = ValidateTransactions.isTxnValid(dto("t1", homer, homer));

        assertTrue(txn.isInvalid());
        assertEquals(1, txn.getErrors().size());
        assertTrue(txn.getErrors().get(0).contains(SAME_USER));
    }

    @Test
    void errorMessageHasTheSenderAndReceiverIds() {
        var txn = ValidateTransactions.isTxnValid(dto("t1", null, user("2", "Marge")));

        assertTrue(txn.getErrors().get(0).contains("to:2"));
    }
}
