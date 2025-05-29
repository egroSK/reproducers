package com.reproducer.model;

import java.math.BigDecimal;

public class TransactionsGroupBy {

    private BigDecimal amount;
    private Integer count;

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "TransactionsGroupBy[amount=%s, count=%s]".formatted(amount, count);
    }

}