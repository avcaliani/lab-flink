package br.avcaliani.lard_lad_exchange.pipelines;

import br.avcaliani.lard_lad_exchange.models.in.Transaction;
import br.avcaliani.lard_lad_exchange.models.in.User;
import br.avcaliani.lard_lad_exchange.models.out.DTOTransaction;

/**
 * Test data builders. Everything here is fake data.
 */
final class Fixtures {

    private Fixtures() {
    }

    static User user(String id, String name) {
        var user = new User();
        user.setId(id);
        user.setName(name);
        return user;
    }

    static Transaction txn(String id, String from, String to) {
        var txn = new Transaction();
        txn.setId(id);
        txn.setFrom(from);
        txn.setTo(to);
        txn.setCurrency("DONU");
        txn.setAmount(10.0);
        txn.setFee(0.5);
        return txn;
    }

    static DTOTransaction dto(String id, User sender, User receiver) {
        var from = sender == null ? "unknown-from" : sender.getId();
        var to = receiver == null ? "unknown-to" : receiver.getId();
        return new DTOTransaction(txn(id, from, to), sender, receiver);
    }
}
