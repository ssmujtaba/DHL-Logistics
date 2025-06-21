# 🚚 DHL Logistics & Supply Chain Dashboard

This repository showcases an end-to-end data analytics project focused on the logistics and supply chain sector. The project involved a complete ETL (Extract, Transform, Load) process: generating and cleaning 10,000 rows of realistic data with Python, creating a normalized database schema in MySQL, and building a multi-page, interactive dashboard with Microsoft Power BI to uncover operational insights.

---

## 🎯 Project Goal

The primary goal was to create a comprehensive BI tool for a logistics company like DHL to monitor and improve its delivery network. The dashboard is designed to answer critical business questions related to on-time performance, shipping costs, and carrier efficiency, enabling managers to identify bottlenecks, optimize routes, and make data-driven decisions to enhance operational performance.

---

## 🛠️ Technical Skills and Process

- **ETL Pipeline (Python & SQL)**:  
  A Python script was developed using the `pandas` library to generate, clean, and transform messy, real-world data. The script then connected to a MySQL database using the `mysql-connector` library to create a normalized Star Schema (`fact_shipments`, `dim_locations`, `dim_carriers`, `dim_dates`) and load the cleaned data.

- **DAX (Data Analysis Expressions)**:  
  The Power BI data model was enhanced with over 10 DAX measures to calculate crucial logistics KPIs, including:
  - **Time intelligence measures** (e.g., On-Time Delivery % Last Month) to track performance trends.
  - **Operational KPIs** (e.g., On-Time Delivery %, Average Cost per Shipment).
  - **Dynamic conditional formatting** to automatically color-code visuals based on performance targets.

- **Data Visualization & Storytelling**:  
  A three-page report was built with a logical flow, moving from a high-level overview to a granular, diagnostic analysis. The design focuses on clarity, interactivity, and telling a compelling data story.

---

## 📖 The Dashboard: A Page-by-Page Story

The final report consists of three pages, each designed to provide a different layer of analysis.

---

### 1. 📈 Executive Overview

**Purpose**:  
To provide a high-level, "at-a-glance" summary of the entire logistics operation for senior management.

**Key Insights & Visuals**:

- **Core KPIs**: 9,000 Total Shipments handled, resulting in **$2.48M in shipping costs**.
- **Performance Snapshot**: On-Time Delivery is at **79.04%**. A donut chart breaks down shipment statuses, showing **19.9%** delayed shipments.
- **Cost Efficiency**: Average Cost per Shipment is **$276.00**, providing a baseline for cost control.
- **Geographic & Time Trends**: 
  - A map shows shipment volume by **state**.
  - A line chart tracks **daily shipment volume** to identify seasonal peaks.

📸 **Dashboard Image – Executive Overview** 

<img width="1280" alt="1" src="https://github.com/user-attachments/assets/305bd9eb-0000-4ce6-9779-833ba5499bb4" />

---

### 2. 📊 On-Time Performance Analysis

**Purpose**:  
To diagnose delivery performance, answering: “Are we meeting our promises?” and “If not, why?”

**Key Insights & Visuals**:

- **Month-over-Month KPI**: On-Time Delivery for the selected month is **76.88%**, a **6.39% drop** from the previous month.
- **Delays by Carrier**: A conditionally formatted bar chart identifies worst performers — **Quick Haul (514 delays)** and **SpeedyShip (464 delays)** — both flagged in red.
- **Top 30 Delayed Shipping Lanes**: Routes like **CA → TX** and **TX → CA** emerge as major delay sources, suggesting network congestion or carrier-specific inefficiencies.

📸 **Dashboard Image – On-Time Performance**  

<img width="1280" alt="1" src="https://github.com/user-attachments/assets/af344c48-c19d-43a2-a61a-c404055d1a4d" />

---

### 3. 💰 Carrier & Cost Analysis

**Purpose**:  
To analyze cost-effectiveness of shipping partners: “Are we getting what we pay for?”

**Key Insights & Visuals**:

- **Cost vs Volume**: Donut charts show **Quick Haul** handles the largest shipment share (**29.09%**) and cost share (**29.54%**) — suggesting proportional volume-to-cost alignment.
- **Cost-Effectiveness**: Quick Haul is most expensive (**$280.25 per shipment**), while Global Cargo is most affordable (**$268.74**).
- **"Magic Quadrant" – Cost vs On-Time Delivery**:
  - **High-Cost, High-Performance**: Reliable Freight
  - **Low-Cost, Low-Performance**: Unknown
  - **High-Cost, Low-Performance (Problem Quadrant)**: Quick Haul, flagged for immediate action

📸 **Dashboard Image – Carrier & Cost Analysis**  

<img width="1280" alt="1" src="https://github.com/user-attachments/assets/f807d31f-16bb-4123-8307-22a3b171ef40" />

---

## ✨ Conclusion

This project successfully demonstrates an end-to-end data pipeline — from **data simulation and cleaning in Python**, to **relational database modeling in MySQL**, and finally to the creation of a powerful, interactive **Power BI dashboard**. The report provides clear, actionable insights that enable a logistics company to:
- Monitor real-time performance
- Diagnose operational issues
- Optimize costs and delivery efficiency

---
