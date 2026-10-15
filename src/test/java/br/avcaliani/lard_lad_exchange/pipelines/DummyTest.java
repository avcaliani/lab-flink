package br.avcaliani.lard_lad_exchange.pipelines;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DummyTest {

    @Test
    void buildsEmailFromName() {
        assertEquals("homer.simpson@github.com", Dummy.buildEmail("Homer Simpson"));
    }

    @Test
    void removesDotsAndExtraSpaces() {
        assertEquals("homer.j.simpson@github.com", Dummy.buildEmail("  Homer J.  Simpson "));
    }

    @Test
    void returnsNullWhenThereIsNoName() {
        assertNull(Dummy.buildEmail(null));
        assertNull(Dummy.buildEmail("  "));
    }
}
