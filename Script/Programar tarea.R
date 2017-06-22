library(taskscheduleR)

setwd("../Script")

myscript <- system.file("extdata", "ARET carga a SF.R", package = "taskscheduleR")

## run script once within 62 seconds
taskscheduler_create(taskname = "DBFaSF", rscript = myscript, 
                     schedule = "MINUTE", starttime = format(Sys.time() + 62, "%H:%M"))


# Shiny
install.packages("data.table")
install.packages("knitr")
install.packages("miniUI")
install.packages("shiny")
install.packages("taskscheduleR", repos = "http://www.datatailor.be/rcube",
                 type = "source")
