# setwd("S:/DATOS")
# 
# # Importar datos ----------------------------------------------------------
# 
# archivos.dbf <- c("aso_mfincas.DBF", "car_castigada.DBF", "car_cosj.DBF", 
#                   "car_cosj_castigada.DBF", "car_mcre.DBF", "car_pagos.DBF", 
#                   "car_pendien.DBF", "car_pendien_castigada.DBF",
#                   "car_vigente.DBF", "daportes.DBF", "factalma.DBF", 
#                   "malmacen.DBF", "masociado.DBF", "mcuoingr.dbf", 
#                   "mmunicipio.dbf" )

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


# 1.1. Informaci?n "Relación Cartera Asociado": CAR_VIGENTE

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
                         FUN = sum, na.rm = TRUE)
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
vr.actual <- car_vigente$VRCREDITO - ifelse(is.na(car_vigente$vr.pagos),
                                            0,
                                            car_vigente$vr.pagos)
     

fecha <- Sys.Date()
estado <- ifelse(car_vigente$FECHAVEN < fecha, "Vencido", "Vigente")

car_vigente <- data.frame(car_vigente, vr.actual, estado)


# 1.2. Informaci?n "Relaci?n Cartera Asociado": CAR_CASTIGADA

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
vr.pagos <- car_castigada$VRCREDITO - ifelse(is.na(car_castigada$VALORCASTI),
                                             0,
                                             car_castigada$VALORCASTI)
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

## Variables iniciales

vars.movCapital <- c("CEDULA", "PERIODO", "CAPINGRE", "CAPANUAL", "DESCPTMO",
                     "CAPCAFE", "REVALORI", "KILCAFE")
movCapital <- daportes.DBF[, vars.movCapital]


## Debe

library(data.table)

f.movCapital <- setDT(movCapital)
colsAgg <- c("CAPINGRE", "CAPANUAL", "DESCPTMO", "CAPCAFE",
             "REVALORI", "KILCAFE")
f.movCapital <- f.movCapital[, lapply(.SD, sum),
                               by=.(CEDULA, PERIODO),
                               .SDcols = colsAgg]
f.movCapital <- setDF(f.movCapital)

debe <- ifelse(80000 - (((f.movCapital$KILCAFE * 6000) * 0.01) * 0.8) > 0,
               80000 - (((f.movCapital$KILCAFE * 6000) * 0.01) * 0.8),
               0)

f.movCapital <- data.frame(f.movCapital, debe)


## Resultado 1 (Estas variables no están en los objetos, por el
## Momento no se van a construir )

# f.movCapital <- data.frame(f.movCapital,
#                            resultado1 = debe - f.movCapital$CAPINGRE)



# 4. OBSERVACIONES --------------------------------------------------------

vars.observaciones <- c("CEDASOCIAD", "OBSERVA1", "OBSERVA2", "OBSERVA3")
f.observaciones <- masociado.DBF[, vars.observaciones]
names(f.observaciones)[names(f.observaciones) == "CEDASOCIAD"] <- "CEDULA"
library(tidyr)
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
                      FUN = sum, na.rm = TRUE)
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

vr.cupo <- gather(vr.cupo, "Tipo", "vr.cupo", Ordinario:Almacen)


##Valor Creditos

# Ordinario
car_vigente_no6 <- car_vigente[car_vigente$CODCREDITO != 6,]

o2.vrCreditos <- aggregate(x = select(car_vigente_no6, vr.actual), 
                           by = select(car_vigente_no6, CEDULA),
                           FUN = sum, na.rm = TRUE)

names(o2.vrCreditos)[2] <- "Ordinario"

# Fertilizante
car_vigente_6 <- car_vigente[car_vigente$CODCREDITO == 6,]

f2.vrCreditos <- aggregate(x = select(car_vigente_6, vr.actual), 
                           by = select(car_vigente_6, CEDULA),
                           FUN = sum, na.rm = TRUE)

names(f2.vrCreditos)[2] <- "Fertilizante"

# Almacen
a2.vrCreditos <- aggregate(x = select(factalma.DBF, VALOR),
                           by = select(factalma.DBF, CEDULA),
                           FUN = sum, na.rm = TRUE)

names(a2.vrCreditos)[2] <- "Almacen"

# Agregar todos los valor creditos
vr.credito <- full_join(o2.vrCreditos, f2.vrCreditos, by = "CEDULA")
vr.credito <- full_join(vr.credito, a2.vrCreditos, by = "CEDULA")
vr.credito <- gather(vr.credito, "Tipo", "vr.credito", Ordinario:Almacen)


## Concatenar valor cupo y valor crédito
f.disponible <- full_join(vr.cupo, vr.credito, by = c("CEDULA", "Tipo"))


## Valor Disponible

# VrActualAfecta (pertenece a totales pero se utiliza acá)
vrActualAfecta <- ifelse(f.RelCartAsoc$NOAFECTACU == 0,
                         0, f.RelCartAsoc$vr.actual)

temp_vr.actual <- data.frame(f.RelCartAsoc, vrActualAfecta)
vrActualAfecta <- aggregate(x = select(temp_vr.actual, vrActualAfecta),
                            by = select(temp_vr.actual, CEDULA),
                            FUN = sum, na.rm = TRUE)

# pruebaNAC <- data.frame(f.RelCartAsoc$NOAFECTACU,
#                         f.RelCartAsoc$vr.actual,
#                         noAfectaCu1,
#                         vrActualAfecta)

## Ordinario
f.disponible <- left_join(f.disponible,
                          select(vrActualAfecta, CEDULA, vrActualAfecta),
                          by = "CEDULA")

vr.disponible <- ifelse(f.disponible$Tipo == "Ordinario",
                        ifelse(is.na(f.disponible$vr.cupo), 
                               0, f.disponible$vr.cupo) -
                             ifelse(is.na(f.disponible$vr.credito),
                                    0, f.disponible$vr.credito) +
                             ifelse(is.na(f.disponible$vrActualAfecta),
                                    0, f.disponible$vrActualAfecta),
                        ifelse(is.na(f.disponible$vr.cupo), 
                               0, f.disponible$vr.cupo) -
                             ifelse(is.na(f.disponible$vr.credito),
                                    0, f.disponible$vr.credito))

f.disponible <- data.frame(f.disponible, vr.disponible)

f.disponible <- select(f.disponible, -matches("vrActualAfecta"))


# 6. TOTALES --------------------------------------------------------------

## Total Cartera Vencida
t.carVencida <- aggregate(x = select(f.RelCartAsoc[f.RelCartAsoc$estado == "Vencido",],
                                     vr.actual), 
                          by = select(f.RelCartAsoc[f.RelCartAsoc$estado == "Vencido",],
                                      CEDULA),
                          FUN = sum,
                          na.rm = TRUE)
names(t.carVencida)[2] <- "car.vencida"
f.totales <- t.carVencida

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

### Interés pendiente

t.intPend <- aggregate(x = select(car_pendien.DBF[car_pendien.DBF$ESTADO != 1, ],
                                        VALOR, VRPAGOS),
                             by = select(car_pendien.DBF[car_pendien.DBF$ESTADO != 1, ],
                                         CEDULA),
                             FUN = sum, na.rm = TRUE)

intPend <- ifelse(is.na(t.intPend$VALOR), 
                        0, t.intPend$VALOR) -
     ifelse(is.na(t.intPend$VRPAGOS), 
            0, t.intPend$VRPAGOS)

t.intPend <- data.frame(t.intPend, intPend)
t.intPend <- select(t.intPend, CEDULA, intPend)

### Interés pendiente castigado

t.intPendCast <- aggregate(x = select(car_pendien_castigada.DBF,
                                  VALOR, VRPAGOS),
                       by = select(car_pendien_castigada.DBF,
                                   CEDULA),
                       FUN = sum, na.rm = TRUE)

intPendCast <- ifelse(is.na(t.intPendCast$VALOR), 
                        0, t.intPendCast$VALOR) -
     ifelse(is.na(t.intPendCast$VRPAGOS), 
            0, t.intPendCast$VRPAGOS)

t.intPendCast <- data.frame(t.intPendCast, intPendCast)
t.intPendCast <- select(t.intPendCast, CEDULA, intPendCast)

### Joint a f.totales
f.totales <- left_join(f.totales, t.intPend, by = "CEDULA")
f.totales <- left_join(f.totales, t.intPendCast, by = "CEDULA")


## Costas Judiciales/castigadas

### Costas judiciales

t.costJud <- aggregate(x = select(car_cosj.DBF[car_cosj.DBF$ESTADO != 1, ],
                                  VALOR, VRPAGO),
                       by = select(car_cosj.DBF[car_cosj.DBF$ESTADO != 1, ],
                                   CEDULA),
                       FUN = sum, na.rm = TRUE)

costJud <- ifelse(is.na(t.costJud$VALOR), 
                  0, t.costJud$VALOR) -
     ifelse(is.na(t.costJud$VRPAGO), 
            0, t.costJud$VRPAGO)

t.costJud <- data.frame(t.costJud, costJud)
t.costJud <- select(t.costJud, CEDULA, costJud)

### Costas judiciales castigadas

t.costJudCast <- aggregate(x = select(car_cosj_castigada.DBF,
                                  VALOR, VRPAGO),
                       by = select(car_cosj_castigada.DBF,
                                   CEDULA),
                       FUN = sum, na.rm = TRUE)

costJudCast <- ifelse(is.na(t.costJudCast$VALOR), 
                  0, t.costJudCast$VALOR) -
     ifelse(is.na(t.costJudCast$VRPAGO), 
            0, t.costJudCast$VRPAGO)

t.costJudCast <- data.frame(t.costJudCast, costJudCast)
t.costJudCast <- select(t.costJudCast, CEDULA, costJudCast)

### Joint a f.totales
f.totales <- left_join(f.totales, t.costJud, by = "CEDULA")
f.totales <- left_join(f.totales, t.costJudCast, by = "CEDULA")

 
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
debeAportes <- aggregate(x = select(f.movCapital, debe),
                         by = select(f.movCapital, CEDULA),
                         FUN = sum, na.rm = TRUE)

f.totales <- left_join(f.totales, debeAportes, by = "CEDULA")

## Total deuda
t.deuda <- rowSums(select(f.totales, cartera, intPend, intPendCast,
                          costJud, costJudCast, debe), na.rm = TRUE)
f.totales <- data.frame(f.totales, t.deuda)

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

f.totales <- f.totales[order(f.totales$Farmer__c), ]

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

# Descargar registros de 'Crédito total'
campos.ct <- c("Cedula__c", "Id")
credito.total <- rforcecom.retrieve(session, "credito_Total__c", campos.ct)
credito.total$Cedula__c <- gsub(".0$", "", 
                                credito.total$Cedula__c)
credito.total$Cedula__c <- as.numeric(credito.total$Cedula__c)
credito.total$Cedula__c <- as.factor(credito.total$Cedula__c)

# Preparar datos
f.observaciones$CEDULA <- as.factor(f.observaciones$CEDULA)
f.observaciones <- inner_join(f.observaciones, credito.total,
                              by = c("CEDULA" = "Cedula__c"))
f.observaciones <- select(f.observaciones, -matches("CEDULA"))
names(f.observaciones) <- c("Observa__c", "credito_Total__c")

f.observaciones <- f.observaciones[order(f.observaciones$credito_Total__c), ]

# Cargar datos

job_info <- rforcecom.createBulkJob(session, 
                                    operation='insert', 
                                    object='Observaciones__c')

batches_info <- rforcecom.createBulkBatch(session, 
                                          jobId=job_info$id, 
                                          f.observaciones, 
                                          multiBatch = TRUE, 
                                          batchSize=500)

# batches_status <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.checkBatchStatus(session, 
#                                                          jobId=x$jobId, 
#                                                          batchId=x$id)
#                          })
# 
# batches_detail <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session, 
#                                                         jobId=x$jobId, 
#                                                         batchId=x$id)
#                          })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)


# 11. CARGAR FACTALMA -----------------------------------------------------

# Preparar datos
f.almacCafe$CEDULA <- as.factor(f.almacCafe$CEDULA)
f.almacCafe <- inner_join(f.almacCafe, credito.total,
                              by = c("CEDULA" = "Cedula__c"))

f.almacCafe <- select(f.almacCafe, -one_of(c("CEDULA", "REFERENCIA",
                                            "NOMALMACEN")))

names(f.almacCafe) <- c("ALMACEN__c", "FECHA__c", "FECVEN__c",
                        "VALOR__c", "Estado_o__c", "credito_Total__c")

f.almacCafe <- f.almacCafe[order(f.almacCafe$credito_Total__c), ]

# Cargar datos

job_info <- rforcecom.createBulkJob(session,
                                    operation='insert', 
                                    object='credito_factalma__c')

batches_info <- rforcecom.createBulkBatch(session, 
                                          jobId=job_info$id, 
                                          f.almacCafe, 
                                          multiBatch = TRUE, 
                                          batchSize=500)

# batches_status <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.checkBatchStatus(session, 
#                                                          jobId=x$jobId, 
#                                                          batchId=x$id)
#                          })
# 
# batches_detail <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session, 
#                                                         jobId=x$jobId, 
#                                                         batchId=x$id)
#                          })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)

# 12. CARGAR DISPONIBLE ---------------------------------------------------

# #### EN CONSTRUCCIÓN!!!!!!!!
# ###
# # Preparar datos
# f.disponible$CEDULA <- as.factor(f.disponible$CEDULA)
# f.disponible <- inner_join(f.disponible, credito.total,
#                           by = c("CEDULA" = "Cedula__c"))
# 
# f.disponible <- select(f.disponible, -one_of(c("CEDULA", "REFERENCIA",
#                                              "NOMALMACEN")))
# 
# names(f.disponible) <- c("credito_Total__c")
# 
# f.disponible <- f.disponible[order(f.disponible$credito_Total__c), ]
# 
# # Cargar datos
# 
# job_info <- rforcecom.createBulkJob(session,
#                                     operation='insert', 
#                                     object='credito_factalma__c')
# 
# batches_info <- rforcecom.createBulkBatch(session, 
#                                           jobId=job_info$id, 
#                                           f.almacCafe, 
#                                           multiBatch = TRUE, 
#                                           batchSize=500)
# 
# batches_status <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.checkBatchStatus(session, 
#                                                          jobId=x$jobId, 
#                                                          batchId=x$id)
#                          })
# 
# batches_detail <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session, 
#                                                         jobId=x$jobId, 
#                                                         batchId=x$id)
#                          })
# 
# close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)

# 13. CARGAR DAPORTES (MOVCAPITAL) ----------------------------------------

# Preparar datos
f.movCapital$CEDULA <- as.factor(f.movCapital$CEDULA)
f.movCapital <- inner_join(f.movCapital, credito.total,
                          by = c("CEDULA" = "Cedula__c"))

f.movCapital <- select(f.movCapital, -one_of(c("CEDULA")))

names(f.movCapital) <- c("PERIODO__c", "CAPINGRESO__c", "CAPANUAL__c",
                         "DESCPTMO__c", "CAPCAFE__c", "REVALORI__c",
                         "KILCAFE__c","credito_Total__c")

f.movCapital <- f.movCapital[order(f.movCapital$credito_Total__c), ]

# Cargar datos

job_info <- rforcecom.createBulkJob(session,
                                    operation='insert', 
                                    object='credito_daportes__c')

batches_info <- rforcecom.createBulkBatch(session, 
                                          jobId=job_info$id, 
                                          f.movCapital, 
                                          multiBatch = TRUE, 
                                          batchSize=500)

batches_status <- lapply(batches_info, 
                         FUN=function(x){
                              rforcecom.checkBatchStatus(session, 
                                                         jobId=x$jobId, 
                                                         batchId=x$id)
                         })

batches_detail <- lapply(batches_info, 
                         FUN=function(x){
                              rforcecom.getBatchDetails(session, 
                                                        jobId=x$jobId, 
                                                        batchId=x$id)
                         })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)

# 14. CARGAR CAR_VIGENTE --------------------------------------------------

# Preparar datos
f.RelCartAsoc$CEDULA <- as.factor(f.RelCartAsoc$CEDULA)
f.RelCartAsoc <- inner_join(f.RelCartAsoc, credito.total,
                           by = c("CEDULA" = "Cedula__c"))

f.RelCartAsoc <- select(f.RelCartAsoc, -one_of(c("CEDULA", "MUNICIPIO",
                                                 "NOAFECTACU")))

names(f.RelCartAsoc) <- c("CODCREDITO__c", "NUMERO__c", "FECHACRE__c",
                          "FECHAVEN__c", "VRCREDITO__c", "REFINANCIA__c",
                          "DESCREDITO__c", "VRPAGOS__c", "VRACTUAL__c",
                          "Estado__c", "credito_Total__c")

f.RelCartAsoc <- f.RelCartAsoc[order(f.RelCartAsoc$credito_Total__c), ]

# Cargar datos

job_info <- rforcecom.createBulkJob(session,
                                    operation='insert', 
                                    object='credito_car_vigente__c')

batches_info <- rforcecom.createBulkBatch(session, 
                                          jobId=job_info$id, 
                                          f.RelCartAsoc, 
                                          multiBatch = TRUE, 
                                          batchSize=500)

# batches_status <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.checkBatchStatus(session, 
#                                                          jobId=x$jobId, 
#                                                          batchId=x$id)
#                          })
# 
# batches_detail <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session, 
#                                                         jobId=x$jobId, 
#                                                         batchId=x$id)
#                          })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)


