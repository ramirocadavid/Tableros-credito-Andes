library(taskscheduleR)

# Prueba
taskscheduler_create(rscript = "C:/Creditos-Salesforce/Test-R-scheduler/prueba_scheduleR.R",
                     schedule = "DAILY", days = "*", starttime = "16:43",
                     startdate = format(Sys.Date(), "%d/%m/%Y"), debug = TRUE)

# Tablero de créditos
taskscheduler_create(rscript = "C:/Creditos-Salesforce/ARET-carga-a-SF.R",
                     schedule = "DAILY", days = "*", starttime = "02:00",
                     startdate = format(Sys.Date(), "%d/%m/%Y"), debug = TRUE)