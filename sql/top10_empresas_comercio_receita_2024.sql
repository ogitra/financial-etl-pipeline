/* =========================================================
   OBJETIVO DA QUERY
   ---------------------------------------------------------
   - Identificar as TOP 10 empresas do SETOR DE COMÉRCIO
     com base na RECEITA LÍQUIDA do ano de 2024
   - Para essas empresas:
         trazer todas as contas selecionadas
         para todos os anos entre 2022 e 2024
   - Eliminar duplicidades estruturais:
         balanços consolidados vs individuais
         múltiplos cadastros de empresas
   ========================================================= */

WITH balancos_normalizados AS (

    /* =====================================================
       1) NORMALIZAÇÃO DOS BALANÇOS
       -----------------------------------------------------
       - Padroniza valores monetários
       - Remove duplicidades de balanço:
           mesma empresa + mesmo ano + mesma conta
       - Prioriza balanço CONSOLIDADO quando disponível
       ===================================================== */
    SELECT
        b.IdPessoaJuridica,
        b.DataFechamento,
        bc.IdConta,

        /* Padronização monetária */
        CASE
            WHEN um.IdUnidadeMonetaria = -3 THEN bc.Valor * 1000000
            WHEN um.IdUnidadeMonetaria = -2 THEN bc.Valor * 1000
            ELSE bc.Valor
        END AS ValorPadronizado,

        /* Regra de prioridade entre balanços */
        ROW_NUMBER() OVER (
            PARTITION BY
                b.IdPessoaJuridica,
                b.DataFechamento,
                bc.IdConta
            ORDER BY
                CASE
                    WHEN b.IdNaturezaBalanco = -2 THEN 1   -- Consolidado
                    WHEN b.IdNaturezaBalanco = -1 THEN 2   -- Individual
                    ELSE 3
                END
        ) AS rn

    FROM Balanco b
    JOIN BalancoConta bc
        ON bc.IdBalanco = b.IdBalanco
    JOIN UnidadeMonetaria um
        ON b.IdUnidadeMonetaria = um.IdUnidadeMonetaria

    WHERE
        b.IdPeriodoBalanco = -3                         -- balanço anual
        AND b.DataFechamento BETWEEN '2022-12-31' AND '2024-12-31'
        AND bc.IdConta IN (
            55, 56, 57, 73, 103,
            178, 230, 286,
            332, 333, 335, 347, 375,
            421, 462, 473
        )
        AND bc.Valor IS NOT NULL
        AND bc.Valor <> 0
),

/* =========================================================
   2) BALANÇOS FILTRADOS
   ---------------------------------------------------------
   Mantém apenas um registro por:
   empresa + ano + conta
   após aplicação da regra de prioridade
   ========================================================= */
balancos AS (
    SELECT
        IdPessoaJuridica,
        DataFechamento,
        IdConta,
        ValorPadronizado
    FROM balancos_normalizados
    WHERE rn = 1
),

/* =========================================================
   3) EMPRESAS DO SETOR DE COMÉRCIO
   ---------------------------------------------------------
   - Garante 1 linha por empresa
   - Remove duplicidade de cadastro
   ========================================================= */
empresas_comercio_ranked AS (
    SELECT
        dc.IdPessoaJuridica,
        dc.NomeFantasia,
        ROW_NUMBER() OVER (
            PARTITION BY dc.IdPessoaJuridica
            ORDER BY dc.NomeFantasia
        ) AS rn
    FROM DadoCadastral dc
    JOIN EmpresaAtividade ea
        ON dc.IdPessoaJuridica = ea.IdPessoaJuridica
    JOIN SetorAtividade sa
        ON sa.IdSetorAtividade = ea.IdSetorAtividade
    WHERE sa.Descricao = 'Setor de comércio'
),

empresas_comercio AS (
    SELECT
        IdPessoaJuridica,
        NomeFantasia
    FROM empresas_comercio_ranked
    WHERE rn = 1
),

/* =========================================================
   4) RECEITA LÍQUIDA DE 2024
   ---------------------------------------------------------
   Base exclusiva para o ranking das empresas.
   Nenhuma outra conta ou ano entra no critério.
   ========================================================= */
receita_2024 AS (
    SELECT
        b.IdPessoaJuridica,
        b.ValorPadronizado AS ReceitaLiquida2024
    FROM balancos b
    WHERE
        b.IdConta = 332                     -- Receita líquida
        AND b.DataFechamento = '2024-12-31'
),

/* =========================================================
   5) TOP 10 EMPRESAS POR RECEITA LÍQUIDA EM 2024
   ---------------------------------------------------------
   Define o conjunto FECHADO de empresas que
   será utilizado na expansão final.
   ========================================================= */
top_10_empresas AS (
    SELECT TOP 10
        r.IdPessoaJuridica
    FROM receita_2024 r
    JOIN empresas_comercio ec
        ON ec.IdPessoaJuridica = r.IdPessoaJuridica
    ORDER BY r.ReceitaLiquida2024 DESC
)

/* =========================================================
   6) RESULTADO FINAL
   ---------------------------------------------------------
   Para as TOP 10 empresas:
   - todas as contas selecionadas
   - todos os anos (2022 a 2024)
   ========================================================= */
SELECT
    b.IdPessoaJuridica AS IdEmpresa,
    ec.NomeFantasia,
    b.DataFechamento,
    b.IdConta,
    c.Codigo     AS CodigoConta,
    c.Descricao  AS ContaDescricao,
    b.ValorPadronizado
FROM balancos b
JOIN top_10_empresas t
    ON t.IdPessoaJuridica = b.IdPessoaJuridica
JOIN empresas_comercio ec
    ON ec.IdPessoaJuridica = b.IdPessoaJuridica
JOIN Conta c
    ON c.IdConta = b.IdConta
ORDER BY
    ec.NomeFantasia,
    b.ValorPadronizado DESC,
    b.DataFechamento
