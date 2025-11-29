/*
    CONSULTA USADA NA ETAPA "EXTRACT" DO PIPELINE ETL
    -----------------------------------------------------------------------
    • Este SQL demonstra como extrair dados contábeis de diversas tabelas,
      padronizar valores monetários, priorizar versões consolidadas e gerar um arquivo para posterior
      tratamento em Python

    • IMPORTANTE:
      Todos os nomes de tabelas, colunas, contas e setores foram
      alterados para fins de portfólio.


    PRINCIPAIS TÓPICOS DEMONSTRADOS:
    --------------------------------
    ✔ Padronização monetária (ex.: multiplicar milhões ou milhares)
    ✔ Seleção apenas de demonstrações financeiras anuais
    ✔ Filtro de contas contábeis relevantes
    ✔ Deduplicação via ROW_NUMBER, priorizando balanço consolidado
    ✔ Uso de múltiplas CTEs para organização
    ✔ Normalização do nome final da empresa (fantasia vs razão social)
    ✔ JOINs entre dados contábeis, empresas e plano de contas
   */


/* ============================================================================
   1) CTE raw_financials
      - Carrega os dados contábeis brutos
      - Padroniza valores conforme unidade monetária
      - Aplica ROW_NUMBER para priorizar CONSOLIDADO > INDIVIDUAL
   ============================================================================ */
WITH raw_financials AS (
    SELECT
        f.IdEmpresa,
        f.DataFechamento,
        fc.IdConta,

        /* Padronização de valores monetários:
           Se a unidade for "MILHÕES" → multiplica por 1.000.000
           Se for "MILHARES"          → multiplica por 1.000
           Caso contrário              → mantém o valor original */
        CASE
            WHEN f.UnidadeMonetaria = 'MILHAO' THEN fc.Valor * 1000000
            WHEN f.UnidadeMonetaria = 'MIL'    THEN fc.Valor * 1000
            ELSE fc.Valor
        END AS ValorPadronizado,

        /* ROW_NUMBER para definir PRIORIDADE entre versões:
           CONSOLIDADO → prioridade 1
           INDIVIDUAL  → prioridade 2
           OUTROS      → prioridade 3
           Assim, na filtragem posterior, ficamos sempre com o consolidado. */
        ROW_NUMBER() OVER (
            PARTITION BY
                f.IdEmpresa,
                f.DataFechamento,
                fc.IdConta
            ORDER BY
                CASE
                    WHEN f.TipoBalanco = 'CONSOLIDADO' THEN 1
                    WHEN f.TipoBalanco = 'INDIVIDUAL'  THEN 2
                    ELSE 3
                END
        ) AS rn

    FROM DemonstracaoFinanceira f
    JOIN DemonstracaoConta fc
        ON fc.IdDemonstracao = f.IdDemonstracao
    WHERE
        f.TipoPeriodo = 'ANUAL'                                     -- Seleciona apenas demonstração anual
        AND f.DataFechamento BETWEEN '2020-12-31' AND '2024-12-31'  -- Range temporal
        AND fc.IdConta IN (                                         -- Contas contábeis relevantes
            55, 56, 57, 73, 103,
            178, 230, 286,
            332, 333, 335, 347, 375,
            421, 462, 473
        )
        AND fc.Valor IS NOT NULL
        AND fc.Valor <> 0
),


/* ============================================================================
   2) CTE financials_filtered
      - Remove duplicações e garante que só fica a linha RN = 1
      - Ou seja, mantemos sempre a versão consolidada quando existir
   ============================================================================ */
financials_filtered AS (
    SELECT
        IdEmpresa,
        DataFechamento,
        IdConta,
        ValorPadronizado
    FROM raw_financials
    WHERE rn = 1
),


/* ============================================================================
   3) CTE companies
      - Normaliza o nome final: NomeFantasia > RazaoSocial
	  - Remove espaços a direita e a esquerda
      - Filtra apenas empresas pertencentes a um setor específico
   ============================================================================ */
companies AS (
    SELECT DISTINCT
        e.IdEmpresa,
		CASE
            WHEN NULLIF(LTRIM(RTRIM(e.NomeFantasia)), '') IS NOT NULL
                THEN LTRIM(RTRIM(e.NomeFantasia))
            ELSE LTRIM(RTRIM(e.RazaoSocial))
        END AS NomeFinal
    FROM Empresa e
    JOIN EmpresaAtividade ea
        ON e.IdEmpresa = ea.IdEmpresa
    JOIN Setor s
        ON ea.IdSetor = s.IdSetor
    WHERE
        s.NomeSetor = 'Setor de comércio'   -- Nome fictício
)


/* ============================================================================
   4) SELECT FINAL
      - Junta dados contábeis + empresas + plano de contas
   ============================================================================ */
SELECT
    f.IdEmpresa,
    c.NomeEmpresa,
    f.DataFechamento,
    f.IdConta,
    pc.CodigoConta,
    pc.DescricaoConta,
    f.ValorPadronizado
FROM financials_filtered f
JOIN companies c
    ON c.IdEmpresa = f.IdEmpresa
JOIN PlanoContas pc
    ON pc.IdConta = f.IdConta
ORDER BY
    c.NomeEmpresa,
    f.DataFechamento DESC,
    f.IdConta;
