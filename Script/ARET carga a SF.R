# Ruta PC oficina
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
library(tidyr)
vr.credito <- gather(vr.credito, "Tipo", "vr.credito", Ordinario:Almacen)


## Concatenar valor cupo y valor crédito




## Valor Disponible

# Ordinario
o.vrDisponible <- o2.vrCupo - o2.vrCreditos$Ordinario

# Fertilizante
f.vrDisponible <- f2.vrCupo - f2.vrCreditos$Fertilizante

# almacen
a.vrDisponible <- a2.vrCupo - a2.vrCreditos$Almacen

# 6. TOTALES --------------------------------------------------------------

## Generar vrActualAfecta
noAfectaCu1 <- f.RelCartAsoc$NOAFECTACU == 1 & !is.na(f.RelCartAsoc$vr.actual)

vrActualAfecta <- ifelse(noAfectaCu1 == FALSE,
                         NA, f.RelCartAsoc$vr.actual)

pruebaNAC <- data.frame(f.RelCartAsoc$NOAFECTACU,
                        f.RelCartAsoc$vr.actual,
                        noAfectaCu1,
                        vrActualAfecta)

temp_vr.actual <- data.frame(f.RelCartAsoc, vrActualAfecta)

vrActualAfecta <- aggregate(x = temp_vr.actual$vrActualAfecta,
                            by = select(temp_vr.actual, CEDULA),
                            FUN = sum,
                            na.rm = TRUE)
# Prueba
temp_vr.actual$vrActualAfecta <-format(temp_vr.actual$vrActualAfecta,
                                       scientific = FALSE)
write.csv(temp_vr.actual, "completo2.csv")
vrActualAfecta$x <-format(vrActualAfecta$x,
                          scientific = FALSE)
write.csv(vrActualAfecta, "agregado2.csv")


f.totales <- vrActualAfecta


## Remover objetos innecesarios para pr?ximos pasos
rm("archivos.dbf", "aso_mfincas.DBF", "car_mcre.DBF", "car_pagos.DBF",
   "car_vigente.DBF", "daportes.DBF", "estado", "estado.almacCafe",
   "factalma.DBF", "fecha", "id.car_pagos", "id.join", "malmacen.DBF",
   "sum.vrPagos", "vars.almacCafe", "vars.car_vigente", "i",
   "vars.movCapital", "vars.observaciones", "vr.actual", "vr.pagos" )

# Subir datos a Salesforce ------------------------------------------------

# # Crear productores nuevos en SF
# 
# # Conexi?n con Salesforce
# library(RForcecom)
# username <- "admin@andes.org"
# password <- "admgf2017*XQWRiDpPU6NzJC9Cmm185FF2"
# instanceURL <- "https://taroworks-8629.cloudforce.com/"
# apiVersion <- "36.0"
# session <- rforcecom.login(username, password, instanceURL, apiVersion)
# 
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

## Crear registros de objeto 'Crédito total'
## https://taroworks-8629.cloudforce.com/01I36000001shlx?setupid=CustomObjects


## Agregar ID de 'Crédito total' a los objetos restantes

### Agregar ID de Salesforce (CORREGIR ESTO, EL ID ES DE 'CRÉDITO TOTAL'!!!!)

f.almacCafe$CEDULA <- as.factor(f.almacCafe$CEDULA)
f.movCapital$CEDULA <- as.factor(f.movCapital$CEDULA)
f.observaciones$CEDULA <- as.factor(f.observaciones$CEDULA)
f.RelCartAsoc$CEDULA <- as.factor(f.RelCartAsoc$CEDULA)
names(productores.sf)[2] <- "CEDULA"

f.almacCafe <- left_join(f.almacCafe, productores.sf, 
                         by = "CEDULA")
f.movCapital <- left_join(f.movCapital, productores.sf, 
                          by = "CEDULA")
f.observaciones <- left_join(f.observaciones, productores.sf, 
                             by = "CEDULA")
f.RelCartAsoc <- left_join(f.RelCartAsoc, productores.sf, 
                           by = "CEDULA")

### Renombrar variables
names(f.almacCafe) <- c("CEDULA", "ALMACEN", "REFERENCIA", "FECHA", "FECVEN",
                        "VALOR", "estado.almacCafe", "Id"  )

### Cargar datos

job_info <- rforcecom.createBulkJob(session,
                                    operation='insert',
                                    object='RForcecom__c')
#### almacCafe
batches_f.almacCafe <- rforcecom.createBulkBatch(session,
                                          jobId=job_info$id,
                                          f.almacCafe,
                                          multiBatch = TRUE,
                                          batchSize=200)
#### movCapital
batches_f.movCapital <- rforcecom.createBulkBatch(session,
                                          jobId=job_info$id,
                                          f.movCapital,
                                          multiBatch = TRUE,
                                          batchSize=200)
#### observaciones
batches_f.observaciones <- rforcecom.createBulkBatch(session,
                                          jobId=job_info$id,
                                          f.observaciones,
                                          multiBatch = TRUE,
                                          batchSize=200)
#### RelCartAsoc
batches_f.RelCartAsoc <- rforcecom.createBulkBatch(session,
                                          jobId=job_info$id,
                                          f.RelCartAsoc,
                                          multiBatch = TRUE,
                                          batchSize=200)
batches_status <- lapply(batches_f.almacCafe,
                         FUN=function(x){
                               rforcecom.checkBatchStatus(session,
                                                          jobId=x$jobId,
                                                          batchId=x$id)
                         })

batches_detail <- lapply(batches_f.almacCafe,
                         FUN=function(x){
                               rforcecom.getBatchDetails(session,
                                                         jobId=x$jobId,
                                                         batchId=x$id)
                         })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)




