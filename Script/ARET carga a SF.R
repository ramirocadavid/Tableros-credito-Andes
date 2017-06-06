setwd("Datos")

# Importar datos ----------------------------------------------------------
archivos.dbf <- list.files(pattern = "*.DBF|*.dbf")

library(foreign)
library(dplyr)

for(i in seq_along(archivos.dbf)) {
     assign(archivos.dbf[i], read.dbf(archivos.dbf[i]))
     assign(archivos.dbf[i], select(get(archivos.dbf[i]),
                                    -starts_with("X")))
}


# 1. RELACIÓN CARTERA ASOCIADO -----------------------------------------------


# 1.1. Informaci?n "Relaci?n Cartera Asociado": CAR_VIGENTE

## Seleccionar variables de car_vigente y crear 'Relaci?n cartera asociado'
vars.car_vigente <- c("CEDULA", "CODCREDITO", "NUMERO", "FECHACRE", "FECHAVEN",
                      "VRCREDITO", "REFINANCIA", "MUNICIPIO")
car_vigente <- car_vigente.DBF[, vars.car_vigente]

## Agregar código de municipio
MUNICIPIO <- substr(mmunicipio.dbf$CODIGO, 1,3)
municipios <- data.frame(MUNICIPIO, mmunicipio.dbf)
municipios <- unique(municipios[, c("MUNICIPIO", "NOMBRE")])
car_vigente <- left_join(car_vigente, select(municipios,
                                             MUNICIPIO, NOMBRE),
                           by = "MUNICIPIO")
car_vigente <- select(car_vigente, -MUNICIPIO)
names(car_vigente)[names(car_vigente) == "NOMBRE"] <- "MUNICIPIO"

## Agregar Nombre de crédito
car_vigente <- left_join(car_vigente, 
                         car_mcre.DBF[, c("CODCREDITO",
                                          "DESCREDITO",
                                          "NOAFECTACU")],
                         by = "CODCREDITO")

## Generar Vr.pagos
vr.pagos <- car_pagos.DBF[, c("CEDULA", "CODCREDITO",
                                  "NUMEROCRE", "CREDITO")]
id.car_pagos <- paste(vr.pagos[, 1], vr.pagos[, 2], vr.pagos[, 3])
id.car_pagos <- as.factor(id.car_pagos)
vr.pagos <- data.frame(id.car_pagos, vr.pagos)
sum.vrPagos <- aggregate(x = select(vr.pagos, CREDITO),
                         by = select(vr.pagos, id.car_pagos),
                         FUN = sum)
names(sum.vrPagos) <- c("id.join", "vr.pagos")

## Prueba vr.pagos
# pr.vr.pagos <- vr.pagos
# pr.vr.pagos$CREDITO <-format(vr.pagos$CREDITO, scientific = FALSE)
# write.csv(pr.vr.pagos, "completo.csv")
# sum.vrPagos$CREDITO <-format(sum.vrPagos$CREDITO, scientific = FALSE)
# write.csv(sum.vrPagos, "agregado.csv")


## Agregar variables de car_pagos a 'Relaci?n cartera asociado'
id.join <- paste(car_vigente[, "CEDULA"],
                        car_vigente[, "CODCREDITO"],
                        car_vigente[, "NUMERO"])
id.join <- as.factor(id.join)
car_vigente <- data.frame(id.join, car_vigente)

car_vigente <- left_join(car_vigente, sum.vrPagos, by = "id.join")
# table(is.na(car_vigente$vr.pagos)) # 1520 (7%) en blanco

## Generar Vr.actual y estado, y agregarlos a 'Relaci?n cartera asociado'
vr.actual <- car_vigente$VRCREDITO - car_vigente$vr.pagos

fecha <- Sys.Date()
estado <- ifelse(car_vigente$FECHAVEN < fecha, "Vencido", "Vigente")

car_vigente <- data.frame(car_vigente, vr.actual, estado)


# 1.2. Informaci?n "Relaci?n Cartera Asociado": CAR_VIGENTE

## Seleccionar variables de car_vigente y crear 'Relaci?n cartera asociado'
vars.car_castigada <- c(vars.car_vigente, "VALORCASTI")
car_castigada <- car_castigada.DBF[, vars.car_castigada]

## Agregar código de municipio
car_castigada <- left_join(car_castigada, municipios, by = "MUNICIPIO")
car_castigada <- select(car_castigada, -matches("MUNICIPIO"))
names(car_castigada)[names(car_castigada) == "NOMBRE"] <- "MUNICIPIO"

## Agregar Nombre de crédito
car_castigada <- left_join(car_castigada, car_mcre.DBF[, c("CODCREDITO",
                                                           "DESCREDITO",
                                                           "NOAFECTACU")],
                           by = "CODCREDITO")

## Generar Vr.pagos y vr.actual
vr.pagos <- car_castigada$VRCREDITO - car_castigada$VALORCASTI
car_castigada <- data.frame(car_castigada, vr.pagos)
names(car_castigada)[names(car_castigada) == "VALORCASTI"] <- "vr.actual"

## Generar estado
estado <- rep("Castigado", nrow(car_castigada))
car_castigada <- data.frame(car_castigada, estado)


# 1.3. Concatenar car_vigente y car_castigada
car_vigente <- select(car_vigente, -matches("id.join"))
orden.car_vigente <- names(car_vigente)
car_castigada <- car_castigada[, orden.car_vigente]
f.RelCartAsoc <- rbind(car_vigente, car_castigada)



# 2. ALMACENES DE CAFÉ ----------------------------------------------------

## Seleccionar variables en factalma 
vars.almacCafe <- c("CEDULA", "ALMACEN", "REFERENCIA", 
                    "FECHA", "FECVEN", "VALOR")
f.almacCafe <- factalma.DBF[, vars.almacCafe]

## Crear variable de estados y agregarla 'Almacenes de caf?'
fecha <- Sys.Date()
estado.almacCafe <- ifelse(f.almacCafe$FECVEN < fecha, "Vencida", "Vigente")
f.almacCafe <- data.frame(f.almacCafe, estado.almacCafe)

## Agregar nombre de almacén
f.almacCafe <- left_join(f.almacCafe, 
                         malmacen.DBF[, c("CODALMACEN", "NOMALMACEN")],
                         by = c("ALMACEN" = "CODALMACEN"))


# 3. MOVIMIENTO DETALLADO CAPITAL ASOCIADO --------------------------------

vars.movCapital <- c("CEDULA", "PERIODO", "CAPINGRE", "CAPANUAL", "DESCPTMO",
                     "CAPCAFE", "REVALORI", "KILCAFE", "CAPINGRE")
f.movCapital <- daportes.DBF[, vars.movCapital]

# ## Debe (preguntar a Liliana)
# id.mcuoingr <- paste(mcuoingr.dbf$MUNICIPIO, mcuoingr.dbf$PERIODO,
#                      mcuoingr.dbf$FECHA)
# mcuoingr <- data.frame(id.mcuoingr, mcuoingr.dbf$CAPITAL)



# 4. OBSERVACIONES --------------------------------------------------------

vars.observaciones <- c("CEDASOCIAD", "OBSERVA1", "OBSERVA2", "OBSERVA3")
f.observaciones <- masociado.DBF[, vars.observaciones]
names(f.observaciones)[names(f.observaciones) == "CEDASOCIAD"] <- "CEDULA"
f.observaciones <- gather(f.observaciones, borrar, Observaciones,
                          OBSERVA1:OBSERVA3)
f.observaciones <- select(f.observaciones, -matches("borrar"))
f.observaciones <- f.observaciones[!is.na(f.observaciones$Observaciones), ]

# 5. DISPONIBLE -----------------------------------------------------------

## Valor cupo

# Ordinario
o0.vrCupo <- aggregate(x = select(daportes.DBF, CAPINGRE, CAPANUAL,
                                 DESCPTMO, CAPCAFE, REVALORI),
                      by = select(daportes.DBF, CEDULA),
                      FUN = sum)
o1.vrCupo <- rowSums(o0.vrCupo[, 2:5], na.rm = TRUE)
o2.vrCupo <- o1.vrCupo * 3

# Fertilizante
f2.vrCupo <- o1.vrCupo * 2

# Almacen
a2.vrCupo <- rowSums(o0.vrCupo[, 2:6], na.rm = TRUE)

# Agregar todos los valor cupo
vr.cupo <- data.frame(o0.vrCupo$CEDULA, o2.vrCupo, f2.vrCupo, a2.vrCupo)
noms.disponible <- c("CEDULA", "Ordinario", "Fertilizante", "Almacen") 
names(vr.cupo) <- noms.disponible

library(tidyr)
vr.cupo <- gather(vr.cupo, "Tipo", "vr.cupo", Ordinario:Almacen)


##Valor Creditos

# Ordinario
o2.vrCreditos <- aggregate(x = select(car_vigente[car_vigente$CODCREDITO != 6,],
                                      vr.actual), 
                           by = select(car_vigente[car_vigente$CODCREDITO != 6,],
                                       CEDULA),
                           FUN = sum)
names(o2.vrCreditos)[2] <- "Ordinario"

# Fertilizante
f2.vrCreditos <- aggregate(x = select(car_vigente[car_vigente$CODCREDITO == 6,],
                                      vr.actual), 
                           by = select(car_vigente[car_vigente$CODCREDITO == 6,],
                                       CEDULA),
                           FUN = sum)
names(f2.vrCreditos)[2] <- "Fertilizante"

# Almacen
a2.vrCreditos <- aggregate(x = select(factalma.DBF, VALOR),
                           by = select(factalma.DBF, CEDULA),
                           FUN = sum)
names(a2.vrCreditos)[2] <- "Almacen"

# Agregar todos los valor creditos
vr.credito <- full_join(o2.vrCreditos, f2.vrCreditos, by = "CEDULA")
vr.credito <- full_join(vr.credito, a2.vrCreditos, by = "CEDULA")
vr.credito <- gather(vr.credito, "Tipo", "vr.credito", Ordinario:Almacen)


## Concatenar valor cupo y valor crédito
f.disponible <- full_join(vr.cupo, vr.credito, by = c("CEDULA", "Tipo"))


## Valor Disponible (actualizar con vrActualAfecta!!!!)

vr.disponible <- ifelse(f.disponible$Tipo == "Ordinario",
                        f.disponible$vr.cupo - f.disponible$vr.credito,
                        f.disponible$vr.cupo - f.disponible$vr.credito)



# 6. TOTALES --------------------------------------------------------------

## Generar vrActualAfecta

noAfectaCu1 <- f.RelCartAsoc$NOAFECTACU == 1 & !is.na(f.RelCartAsoc$vr.actual)

vrActualAfecta <- ifelse(noAfectaCu1 == FALSE,
                         NA, f.RelCartAsoc$vr.actual)

# pruebaNAC <- data.frame(f.RelCartAsoc$NOAFECTACU,
#                         f.RelCartAsoc$vr.actual,
#                         noAfectaCu1,
#                         vrActualAfecta)

temp_vr.actual <- data.frame(f.RelCartAsoc, vrActualAfecta)
vrActualAfecta <- aggregate(x = select(temp_vr.actual, vrActualAfecta),
                            by = select(temp_vr.actual, CEDULA),
                            FUN = sum,
                            na.rm = TRUE)

# # Prueba
# temp_vr.actual$vrActualAfecta <-format(temp_vr.actual$vrActualAfecta,
#                                        scientific = FALSE)
# write.csv(temp_vr.actual, "completo2.csv")
# vrActualAfecta$x <-format(vrActualAfecta$x,
#                           scientific = FALSE)
# write.csv(vrActualAfecta, "agregado2.csv")

f.totales <- vrActualAfecta


## Total Cartera Vencida
t.carVencida <- aggregate(x = select(f.RelCartAsoc[f.RelCartAsoc$estado == "Vencido",],
                                     vr.actual), 
                          by = select(f.RelCartAsoc[f.RelCartAsoc$estado == "Vencido",],
                                      CEDULA),
                          FUN = sum,
                          na.rm = TRUE)
names(t.carVencida)[2] <- "car.vencida"
f.totales <- full_join(f.totales, t.carVencida, by = "CEDULA")

## Total Cartera Vigente
t.carVigente <- aggregate(x = select(f.RelCartAsoc[f.RelCartAsoc$estado == "Vigente",],
                                     vr.actual), 
                          by = select(f.RelCartAsoc[f.RelCartAsoc$estado == "Vigente",],
                                      CEDULA),
                          FUN = sum,
                          na.rm = TRUE)
names(t.carVigente)[2] <- "car.vigente"
f.totales <- full_join(f.totales, t.carVigente, by = "CEDULA")


## Total cartera
f.totales <- data.frame(f.totales,
                        cartera = rowSums(select(f.totales,
                                                 car.vencida,
                                                 car.vigente),
                                          na.rm = TRUE))

## Interés Pendiente/Interés Pendiente Castigado
 
## Costas Judiciales
 
## Almacén Vencido
t.almVencido <- aggregate(x = select(f.almacCafe[f.almacCafe$estado.almacCafe == "Vencida",],
                                     VALOR),
                          by = select(f.almacCafe[f.almacCafe$estado.almacCafe == "Vencida",],
                                      CEDULA),
                          FUN = sum,
                          na.rm = TRUE)
names(t.almVencido)[2] <- "almac.vencida"
f.totales <- left_join(f.totales, t.almVencido, by = "CEDULA")

## Almacén Vigente
t.almVigente <- aggregate(x = select(f.almacCafe[f.almacCafe$estado.almacCafe == "Vigente",],
                                     VALOR),
                          by = select(f.almacCafe[f.almacCafe$estado.almacCafe == "Vigente",],
                                      CEDULA),
                          FUN = sum,
                          na.rm = TRUE)
names(t.almVigente)[2] <- "almac.vigente"
f.totales <- left_join(f.totales, t.almVigente, by = "CEDULA")

## Total almacén
f.totales <- data.frame(f.totales,
                        almacen = rowSums(select(f.totales,
                                                 almac.vencida,
                                                 almac.vigente),
                                          na.rm = TRUE))

## Debe aportes

## Total deuda


## Aportes
t.aportes <- data.frame(select(o0.vrCupo, CEDULA),
                      aportes = rowSums(o0.vrCupo[, 2:5], na.rm = TRUE))

f.totales <- left_join(f.totales, t.aportes, by = "CEDULA")

## Revalorización
t.revalori <- data.frame(select(o0.vrCupo, CEDULA),
                        revalori = o0.vrCupo[, 6])
f.totales <- left_join(f.totales, t.revalori, by = "CEDULA")

## Capital
t.capital <- data.frame(select(o0.vrCupo, CEDULA),
                        capital = rowSums(o0.vrCupo[, 2:6], na.rm = TRUE))

f.totales <- left_join(f.totales, t.capital, by = "CEDULA")



# 7. BORRAR DATOS EXISTENTES EN SALESFORCE --------------------------------

# Parte de esto ya está hecho en el script 'Borrar datos.R'

# 8. CREAR PRODUCTORES NUEVOS EN SALESFORCE -------------------------------

# ## Descargar c?dulas y IDs de objeto farmer
# objectName <- "gfmAg__Farmer__c"
# fields <- c("Id", "Documento_identidad__c")
# productores.sf <- rforcecom.retrieve(session, objectName, fields)
# names(productores.sf)[2] <- "CEDULA"
# class(productores.sf$CEDULA)
# #guardada en RDS como "productores_sf.RDS"
# 
# ## Identificar productores en masociados que no est?n en SF (farmers)
# vars_masociados <- c("CEDASOCIAD", "NOMBRES", "APELLIDO1", "APELLIDO2")
# productores.and <- masociado.DBF[ , vars_masociados]
# names(productores.and) <- c("Documento_identidad__c", "nombres", "apellido1",
#                        "apellido2")
# productores.and$Documento_identidad__c <- as.factor(productores.and$Documento_identidad__c)
# 
# library(dplyr)
# productores.no.sf <- anti_join(productores.and, productores.sf)

## Crear contactos de los farmers (Nombre, Apellido, CC)
# job_info <- rforcecom.createBulkJob(session, 
#                                     operation='insert', 
#                                     object='RForcecom__c')
# 
# batches_info <- rforcecom.createBulkBatch(session, 
#                                           jobId=job_info$id, 
#                                           data, 
#                                           multiBatch = TRUE, 
#                                           batchSize=200)
# 
# batches_status <- lapply(batches_info, 
#                          FUN=function(x){
#                                rforcecom.checkBatchStatus(session, 
#                                                           jobId=x$jobId, 
#                                                           batchId=x$id)
#                          })
# 
# batches_detail <- lapply(batches_info, 
#                          FUN=function(x){
#                                rforcecom.getBatchDetails(session, 
#                                                          jobId=x$jobId, 
#                                                          batchId=x$id)
#                          })
# 
# close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)
# 
# ## Crear persons
# 
# ## Crear farmers



# 9. CARGAR CRÉDITO TOTAL -------------------------------------------------

# Descargar farmers de Salesforce (cédula y id)

library(RForcecom)
username <- "admin@andes.org"
password <- "admgf2017*XQWRiDpPU6NzJC9Cmm185FF2"
instanceURL <- "https://taroworks-8629.cloudforce.com/"
apiVersion <- "36.0"
session <- rforcecom.login(username, password, instanceURL, apiVersion)

fields <- c("Id", "Documento_identidad__c")
productores.sf <- rforcecom.retrieve(session, "gfmAg__Farmer__c", fields)

# Agregar ID de Salesforce a f.totales

f.totales$CEDULA <- as.factor(f.totales$CEDULA)
f.totales <- inner_join(f.totales, productores.sf,
                       by = c("CEDULA" = "Documento_identidad__c"))


# Descripción objeto
# desc.credito_total <- rforcecom.getObjectDescription(session, "credito_Total__c")

# Limpiar y renombrar variables f.totales por API names
f.totales <- f.totales[, c(12, 1, 3:11)]

names(f.totales) <- c("Farmer__c","Cedula__c",  "Cartera_Vencida__c",
                      "Cartera_vigente__c", "Total_cartera__c",
                      "Almacen_vencido__c", "Almacen_vigente__c",
                      "Total_almacen__c", "Aportes__c", "Revalorizacion__c",
                      "Total_Capital__c")


# run an insert job into the Account object
job_info <- rforcecom.createBulkJob(session, 
                                    operation='insert', 
                                    object='credito_Total__c')

# split into batch sizes of 500 (2 batches for our 1000 row sample dataset)
batches_info <- rforcecom.createBulkBatch(session, 
                                          jobId=job_info$id, 
                                          f.totales, 
                                          multiBatch = TRUE, 
                                          batchSize=500)

# check on status of each batch
batches_status <- lapply(batches_info, 
                         FUN=function(x){
                              rforcecom.checkBatchStatus(session, 
                                                         jobId=x$jobId, 
                                                         batchId=x$id)
                         })
# get details on each batch
batches_detail <- lapply(batches_info, 
                         FUN=function(x){
                              rforcecom.getBatchDetails(session, 
                                                        jobId=x$jobId, 
                                                        batchId=x$id)
                         })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)


# 10. CARGAR OBSERVACIONES ------------------------------------------------


# Preparar datos

## Descargar registros de 'Crédito total'
campos.cd <- c("Cedula__c", "Id")
credito.total <- rforcecom.retrieve(session, "credito_Total__c", campos.cd)

## Observaciones
f.observaciones$CEDULA <- as.factor(f.observaciones$CEDULA)
f.observaciones <- inner_join(f.observaciones, credito.total,
                              by = c("CEDULA" = "Cedula__c"))

## AlmacCafe


# Cargar a Salesforce

# Validar