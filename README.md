## Full-stack Data & AI System For Swift, Precise Analytics

### Project Overview

This project is designed with an ultra focus on aligning business requirements and policy constraints with technical scalability.
The objective covers the breadth of a unified analytics and intelligence system that supports customer behavior analysis, decision support, and conversational insights. The solution integrates data engineering, analytics, feature engineering, and AI-driven interfaces, delivered through a backend feature and an intuitive frontend inrerface.

### Technology Stack
- Data & Backend: Python (Pandas, FastAPI)
- Analytics & Feature Engineering: Statistical analysis, clustering (K-Means)
- AI: LLMs, RAG, LangChain, LangGraph
- Frontend: Next.js
- Architecture: ETL pipelines, star schema modeling, multi-agent AI

### Data Engineering & ETL Pipeline
The project began with the design of an ETL pipeline, following standard data engineering best practices.

#### Data Ingestion & Architecture
Raw data was ingested into a data lake–style folder structure to preserve history and serve the medallion architecture:
- Bronze layer: source data ingestion
- Silver layer: Cleaned and standardized datasets with quality checks
- Gold layer: Business-ready fact and dimension tables optimized for analytics
This structure ensures traceability, scalability, and governance for downstream analytics and AI use cases.

#### Data Cleaning & Quality Handling
The Silver layer transformation involved extensive data quality checks, including handling of missing values, duplicates, data types, and schema consistency.
One of such occurrences was in the treatment of the currency feature - Based on domain knowledge of Nordic markets, EUR was treated as the base currency.
Missing values were first replicated into a separate column (currency_imputed) for financial lineage and auditability.. The primary currency column was then imputed with EUR. All currency codes were normalized to uppercase ISO standards.

#### Duplicate Transactions
Approximately 2,000 duplicate transaction_ids were identified. These were investigated using timestamps, transaction amounts, and customer IDs to detect potential late-arriving data or pipeline retries.
Since no consistent pattern justified retaining duplicates, they were safely removed to preserve transactional integrity.
Data Quality Alert
An anomaly was detected where all customer emails appeared identical.
In line with standard data governance practices, this issue was flagged and documented for review as it may indicate upstream masking errors.

#### Data Modeling: Gold Layer
Once the data was cleaned, it was transformed into a star schema to support analytics, reporting, and AI workloads.
Fact & Dimension Design
- Fact table: Transactions (measures such as amount, exchange rate, time attributes)
- Dimension tables: Customers, Dates, Categories, Currencies

#### Slowly Changing Dimension - Type 2
The Customer dimension was modeled as a Slowly Changing Dimension Type 2, anticipating changes such as country or account attributes over time.
This ensures:
- historical accuracy,
- resilience to upstream changes,
- consistency in time-based analytics.
Final quality checks were performed across all fact and dimension tables before exposing the data for downstream consumption.

### Customer Transaction Frequency & Segmentation
Before establishing a many-to-one relationship between the data that drove this line of work, the above features had been Chosen to cover the following metrics and business questions:

#### Transaction Frequency
Chosen to measure customer engagement and loyalty.
Supports: Who are our most engaged customers? Who is at churn risk?
Value: Enables retention strategies and loyalty program design.

#### Total Spend per Customer
Chosen as a proxy for customer lifetime value.
Supports: Who drives most of the revenue? Where should VIP efforts focus?
Value: Guides revenue concentration analysis and premium customer treatment.

#### Average Transaction Amount
Chosen to capture purchasing behavior, not just total value.
Supports: Do customers prefer small frequent purchases or large baskets?
Value: Informs pricing, bundling, and discount strategies.

#### Category Preference
Chosen to understand what customers actually buy.
Supports: Which products drive loyalty and repeat purchases?
Value: Enables personalized marketing and cross-sell opportunities.

#### Customer Segmentation
Chosen to group customers by behavior instead of treating everyone the same.
Supports: How should marketing, pricing, and retention differ by segment?
Value: Turns raw metrics into actionable customer personas.

### AI-Multi-Agentic Decision Support System
The AI solution was architected around a Retrieval-Augmented Generation (RAG) approach, where policy documents and business rules are embedded and retrieved contextually at query time. This ensures that responses are rooted in approved policies and compliance constraints rather than free-form generation.

On top of the RAG layer, a multi-agent architecture was implemented. Each agent is specialized for a distinct responsibility, such as policy interpretation or customer analytics, and exposed as a tool within the system. A supervisory agent, following a ReAct-style reasoning pattern, evaluates the user’s request, determines the appropriate action, and routes the query to the correct agent. This design improves the reliability, transparency, and explainability of responses.

The conversational AI connects directly to the analytics warehouse, enabling natural-language queries over both structured business data and unstructured policy documentation, while enforcing business rules in real time.

<div align="center">
  <img src="https://github.com/fredie7/accenture_assignment/blob/main/prooject-images/ai-tool-calls.png?raw=true" />
  <br>
   <sub><b>AI Tool Calls</b></sub>
</div>

<table align="center">
  <tr>
    <td align="center">
      <img src="https://github.com/fredie7/accenture_assignment/blob/main/prooject-images/conversation_1.png?raw=true" height="300"><br>
      <sub><b>CLient & AI Interaction</b> </sub>
    </td>
    <td align="center">
      <img src="https://github.com/fredie7/accenture_assignment/blob/main/prooject-images/conversation_2.png?raw=true" height="300"><br>
      <sub><b>CLient & AI Interaction</b> </sub>
    </td>
  </tr>
</table>

To operationalize the system, a FastAPI backend was developed to serve as the interface between stakeholders and the AI services. All endpoints were tested for correctness, performance, and reliability. Finally, a Next.js frontend was built to allow non-technical stakeholders to interact with the system conversationally and generate accurate, business-ready insights without requiring technical expertise.
