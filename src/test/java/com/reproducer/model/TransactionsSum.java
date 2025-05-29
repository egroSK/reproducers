package com.reproducer.model;

import java.math.BigDecimal;

public class TransactionsSum {

    private BigDecimal transactionsAmount;

    public BigDecimal getTransactionsAmount() {
        return transactionsAmount;
    }

    public void setTransactionsAmount(BigDecimal transactionsAmount) {
        this.transactionsAmount = transactionsAmount;
    }

    @Override
    public String toString() {
        return "TransactionsSum[transactionsAmount=%s]".formatted(transactionsAmount);
    }

}