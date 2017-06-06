library(RForcecom)
username <- "admin@andes.org"
password <- "admgf2017*XQWRiDpPU6NzJC9Cmm185FF2"
instanceURL <- "https://taroworks-8629.cloudforce.com/"
apiVersion <- "36.0"
session <- rforcecom.login(username, password, instanceURL, apiVersion)

borrar <- rforcecom.retrieve(session, "credito_Total__c", "Id")

# run an insert job into the Account object
job_info <- rforcecom.createBulkJob(session, 
                                    operation='delete', 
                                    object='credito_Total__c')

# split into batch sizes of 500 (2 batches for our 1000 row sample dataset)
batches_info <- rforcecom.createBulkBatch(session, 
                                          jobId=job_info$id, 
                                          borrar, 
                                          multiBatch = TRUE, 
                                          batchSize=500)

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)
