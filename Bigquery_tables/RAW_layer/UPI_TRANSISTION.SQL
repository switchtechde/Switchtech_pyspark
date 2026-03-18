CREATE TABLE project_id.Raw_layer.Upi_transistion (
    Sr_no INT AUTO_INCREMENT,
    Transisition_id STRING NOT NULL PRIMARY KEY,
    Source_upi_id STRING,
    Target_upi_id STRING,
    Transistion_time TIMESTAMP,
    Transistion_date TIMESTAMP,
    UPi_refrence_number STRING,
    Status STRING,
    Sender_bank_name STRING,
    Reciver_bank_name STRING,
    reciver_account_number INT
)
PARTITION BY DATE(Transistion_date)
CLUSTER BY Transisition_id, Source_upi_id;