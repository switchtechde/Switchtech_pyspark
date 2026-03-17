
Bigquery  -  Datawarehouse  -  It will store data All different Type of data in table format  -  
        All different type of data means -  Data genereated from Different type of sources which containg an 
        some none useful information 
        Transformed data  -  In Contain an transformed data 
        Conformed layer -  It contain data which is Useful for Data ANylist team , Data Science Team , Business anyliast ,Stackeholder 

Raw layer  -  It Contain raw data in which we are storing data 
Transformed_layer  -  
Conformed layer  - 
Banking domain  -  

UPI Transistion  




Raw_layer.Upi_transistion -  
    Sr_no   
    Transisition_id  -  this is an Unique  -  It Is an Primary_key 
    Source_upi_id
    Target_upi_id
    Transistion_time 
    Transistion_date 
    UPi refrence number
    Status - 
    Sender_bank_name 
    Reciver_bank_name 
    reciver_account_number  - 

Transformed_layer.Upi_completed 
    It contaion Only Those kind of data where Status of transistion is completed
    Should not contain Dublicate values 
    Should Not contain Null value 
    Should replace first 4 digit and  last 3 digit as X 

    12345678910
    XXXX5678XXX
    
    Sr_no   
    Transisition_id  -  It is an Primary key 
    Source_upi_id
    Target_upi_id
    Transistion_time 
    Transistion_date 
    UPi refrence number
    Status - 
    Sender_bank_name 
    Reciver_bank_name 
    reciver_account_number  - 
    Update_date_time  -  When transistion is Completed 
    Created_date_time -  When Transiistion Is happened 
    Service_provider =  
    Should replace first 4 digit and  last 3 digit as X 


Transformed_layer.Upi_failed  =  
    It contaion Only Those kind of data where Status of  transistion is in progress or failed 
    Should not contain Dublicate values 
    Should Not contain Null value 



    Sr_no   
    Transisition_id  -  It is an Primary key 
    Source_upi_id
    Target_upi_id
    Transistion_time 
    Transistion_date 
    UPi refrence number
    Status - 
    Sender_bank_name 
    Reciver_bank_name 
    reciver_account_number  - 
    Update_date_time  - It will Be Updated datetime 
    Created_date_time - When Transiistion Is happened 
    Service_provider =  



Conformed_layer.Upi_failed 



We are working For  ICICI bank  -  

How phonepay will share a data from Phonepay Datawarehouse to ICICI bank - Phonepay_upi  - 
    Instead of This Phonepay_Upi It will only those trasnsistion which in which sender name or reciver name is ICICI bank 
    It Created an ETL pipeline In which It is reading data from table and Writing in parquet format at specific location 
    And Will give an access of That Bucket .




create an ETL Pipeline which will Extract an data from parquet file and By using some transformation it will Write data into Transformed Layer 







