setwd("C:/Users/Ramiro/rcadavid@grameenfoundation.org/1. MyE/TOOLS/PALANTIR/ARET/Dataset de prueba")

# Pasos -------------------------------------------------------------------

# - Crear en SF todos los productores de masociados que no estén aún en SF
# - Descargar cédulas y ID de todos los farmers en Salesforce
# - Agregar columna de ID a archivos DBF y exportarlos como CSV


# Crear productores nuevos en SF ------------------------------------------

# Conexión con Salesforce
library(RForcecom)
username <- "admin@andes.org"
password <- "admgf2017XQWRiDpPU6NzJC9Cmm185FF2"
instanceURL <- "https://taroworks-8629.cloudforce.com/"
apiVersion <- "36.0"
session <- rforcecom.login(username, password, instanceURL, apiVersion)

# Descargar cédulas y IDs de objeto farmers
objectName <- "gfmAg__Farmer__c"
fields <- c("Id", "Documento_identidad__c")
productores <- rforcecom.retrieve(session, objectName, fields)

# Identificar productores en masociado que no estén en SF (farmers)
library(foreign)
masociados <- read.dbf("masociado.DBF")
vars_masociados <- c("CEDASOCIAD", "NOMBRES", "APELLIDO1", "APELLIDO2") #agregar?
masociados <- masociados[ , vars_masociados]
names(masociados) <- c("Documento_identidad__c", "nombres", "apellido1",
                       "apellido2")
masociados$Documento_identidad__c <- as.factor(masociados$Documento_identidad__c)
library(plyr)
masociados <- join(masociados, productores)
nuevos.asociados <- masociados[is.na(masociados$Id), ]



# Agregar ID de SF --------------------------------------------------------

#Nombres archivos
dbfs <- list.files(pattern = "*.DBF")

#Nombres variables de archivos
for(i in 1:length(dbfs)) {
     assign(paste("nom.", dbfs[i], sep = ""), names(read.dbf(dbfs[i])))
}

# Importar archivos DBF
for(i in 1:length(dbfs)) {
     assign(paste("dbf.", dbfs[i], sep = ""), read.dbf(dbfs[i]))
}

# Todas las variables de cédula con el mismo nombre y tipo factor
########### names(dbf.aso_mfincas.DBF[1]) <- cedula
names(dbf.car_vigente.DBF)[1] <- "Documento_identidad__c"
names(dbf.daportes.DBF)[1] <- "Documento_identidad__c"
names(dbf.factalma.DBF)[7] <- "Documento_identidad__c"
names(dbf.masociado.DBF)[1] <- "Documento_identidad__c"

dbf.car_vigente.DBF$Documento_identidad__c <- as.factor(dbf.car_vigente.DBF$Documento_identidad__c)
dbf.daportes.DBF$Documento_identidad__c <- as.factor(dbf.daportes.DBF$Documento_identidad__c)
dbf.factalma.DBF$Documento_identidad__c <- as.factor(dbf.factalma.DBF$Documento_identidad__c)
dbf.masociado.DBF$Documento_identidad__c <- as.factor(dbf.masociado.DBF$Documento_identidad__c)

# Agregar variable de ID a todos los archivos
dbf.car_vigente.DBF <- join(dbf.car_vigente.DBF, productores)
dbf.daportes.DBF <- join(dbf.daportes.DBF, productores)
dbf.factalma.DBF  <- join(dbf.factalma.DBF, productores)
dbf.masociado.DBF  <- join(dbf.masociado.DBF, productores)

# Verificar cuántos NAs quedaron en cada archivo
table(is.na(dbf.car_vigente.DBF$Id))
table(is.na(dbf.daportes.DBF$Id))
table(is.na(dbf.factalma.DBF$Id))
table(is.na(dbf.masociado.DBF$Id))



# Exportar como CSV -------------------------------------------------------

write.csv(dbf.car_vigente.DBF, "car_vigente.csv")
write.csv(dbf.daportes.DBF, "daportes.csv")
write.csv(dbf.factalma.DBF, "factalma.csv")
write.csv(dbf.masociado.DBF, "masociado.csv")