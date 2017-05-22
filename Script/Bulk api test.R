# Salesforce login
library(RForcecom)
username <- "admin@andes.org"
password <- "admgf2017*XQWRiDpPU6NzJC9Cmm185FF2"
instanceURL <- "https://taroworks-8629.cloudforce.com/"
apiVersion <- "36.0"
session <- rforcecom.login(username, password, instanceURL, apiVersion) 

# Crear datos
ABC__c <- rep(c("a", "b", "c"), 50)
Numero__c <- round(rnorm(150, mean = 10, sd = 2), digits = 0)
Name <- c()
for(i in 1:150) {
      Name <- c(Name, paste("name", i))
}

data <- data.frame(Name, Numero__c, ABC__c)

# Cargar datos

## run an insert job into the Account object
job_info <- rforcecom.createBulkJob(session, 
                                    operation='insert', 
                                    object='RForcecom__c')

## split into batch sizes of 200
batches_info <- rforcecom.createBulkBatch(session, 
                                          jobId=job_info$id, 
                                          data, 
                                          multiBatch = TRUE, 
                                          batchSize=200)

## check on status of each batch
batches_status <- lapply(batches_info, 
                         FUN=function(x){
                               rforcecom.checkBatchStatus(session, 
                                                          jobId=x$jobId, 
                                                          batchId=x$id)
                         })
## get details on each batch
batches_detail <- lapply(batches_info, 
                         FUN=function(x){
                               rforcecom.getBatchDetails(session, 
                                                         jobId=x$jobId, 
                                                         batchId=x$id)
                         })
## close the job
close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)
