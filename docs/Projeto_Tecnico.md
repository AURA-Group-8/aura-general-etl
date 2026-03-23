# 📄 Módulo de ETL – Importação e Padronização de Dados

## 🎯 Objetivo

O módulo de ETL tem como objetivo permitir a importação de arquivos externos contendo dados financeiros da clínica (custos operacionais), garantindo:

- Compatibilidade com múltiplos formatos
- Padronização estrutural
- Tratamento de inconsistências
- Persistência confiável no banco de dados
- Disponibilização para análise e modelos de IA

---

# 📂 Fonte de Dados

O sistema permitirá upload manual via aplicativo da profissional.

Formatos aceitos:

- `.csv`
- `.xlsx`

**Justificativa de negócio:**  
A clínica pode utilizar planilhas externas para controle financeiro e precisa integrar esses dados ao sistema sem digitação manual.

---

# 🔍 Etapas do Processo ETL

## 1️⃣ Extract (Extração)

O módulo:

- Recebe o arquivo
- Identifica automaticamente o formato:
  - Se `.csv` → leitura com `pandas.read_csv()`
  - Se `.xlsx` → leitura com `pandas.read_excel()`
- Converte o conteúdo para DataFrame

### Validações iniciais:

- Arquivo vazio
- Extensão inválida
- Estrutura mínima obrigatória

---