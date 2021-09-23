package com.example.demokafkaconsumernf.model;

import lombok.*;

import java.math.BigDecimal;

@Builder
@Getter
@Setter
@AllArgsConstructor
public class Compra {
    private Integer id;
    private String produto;
    private BigDecimal valor;
}
