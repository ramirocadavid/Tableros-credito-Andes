# 0. IMPORTAR DATOS --------------------------------------------------

# Definir directorio con archivos de SIIA 
setwd("S:/DATOS")

# Lista de archivos a utilizar
archivos.dbf <- c("car_castigada.DBF", "car_cosj.DBF",
                  "car_cosj_castigada.DBF", "car_mcre.DBF", "car_pagos.DBF",
                  "car_pendien.DBF", "car_pendien_castigada.DBF",
                  "car_vigente.DBF", "daportes.DBF", "factalma.DBF",
                  "malmacen.DBF", "masociado.DBF", "mcuoingr.dbf",
                  "mmunicipio.dbf", "maportes.DBF" )

# Librería para leer archivos .dbf (foreign) y manipular objetos (dplyr)
library(foreign)
library(dplyr)

# Importar datos
for(i in seq_along(archivos.dbf)) {
     assign(archivos.dbf[i], read.dbf(archivos.dbf[i]))
     assign(archivos.dbf[i], select(get(archivos.dbf[i]),
                                    -starts_with("X")))
}

# 1. RELACIÓN CARTERA ASOCIADO -----------------------------------------------

# 1.1. Informaci?n "Relación Cartera Asociado": CAR_VIGENTE

## Seleccionar variables de car_vigente y crear 'Relaci?n cartera asociado'
vars.car_vigente <- c("CEDULA", "CODCREDITO", "NUMERO", "FECHACRE", "FECHAVEN",
                      "VRCREDITO", "REFINANCIA", "REESTRUC", "MUNICIPIO",
                      "BORRADOLOG")
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
vr.actual <- ifelse(vr.actual < 0, 0, vr.actual)
     

fecha <- Sys.Date()
estado <- ifelse(car_vigente$FECHAVEN < fecha, "Vencido", "Vigente")

car_vigente <- data.frame(car_vigente, vr.actual, estado)

# Eliminar las que están en car_castigada
car_vigente <- anti_join(car_vigente, car_castigada.DBF,
                         by = "CEDULA")


# 1.2. Informaci?n "Relaci?n Cartera Asociado": CAR_CASTIGADA

## Seleccionar variables de car_vigente y crear 'Relaci?n cartera asociado'
vars.car_castigada <- c(vars.car_vigente[vars.car_vigente != "BORRADOLOG"],
                        "VALORCASTI")
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

# Mover variable BORRADOLOG en car_vigente para el final y eliminar id.join
car_vigente <- select(car_vigente, -BORRADOLOG, BORRADOLOG)
car_vigente <- select(car_vigente, -matches("id.join"))

# Agregar variable BORRADOLOG en car_castigada con valor 2
car_castigada <- data.frame(car_castigada,
                            BORRADOLOG = rep(2, nrow(car_castigada)))

orden.car_vigente <- names(car_vigente)
car_vigente <- data.frame(tipo.credito = rep("Cartera vigente",
                                             nrow(car_vigente)),
                          car_vigente)

car_castigada <- car_castigada[, orden.car_vigente]
car_castigada <- data.frame(tipo.credito = rep("Cartera castigada",
                                               nrow(car_castigada)),
                            car_castigada)

f.RelCartAsoc <- rbind(car_vigente, car_castigada)

## Refinanciado: reemplazar 0 por NA
f.RelCartAsoc$REFINANCIA <- ifelse(is.na(f.RelCartAsoc$REFINANCIA) | 
                                        f.RelCartAsoc$REFINANCIA == 0, NA,
                                   f.RelCartAsoc$REFINANCIA)

## Reestructurado: reemplazar 0 por NA
f.RelCartAsoc$REESTRUC <- ifelse(is.na(f.RelCartAsoc$REESTRUC) | 
                                      f.RelCartAsoc$REESTRUC == 0, NA,
                                 f.RelCartAsoc$REESTRUC)

# # Eliminar registros que no están vigentes (BORRADOLOG == 1 o 2)
# f.RelCartAsoc <- f.RelCartAsoc[is.na(f.RelCartAsoc$BORRADOLOG) |
#                                     f.RelCartAsoc$BORRADOLOG == 0, ]
# # Eliminar variable BORRADOLOG
# f.RelCartAsoc <- select(f.RelCartAsoc, -BORRADOLOG)

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

# Seleccionar Variables iniciales
vars.movCapital <- c("CEDULA", "PERIODO", "CAPINGRE", "CAPANUAL", "SOLANUAL",
                     "DESCPTMO", "CAPCAFE", "SOLCAFE", "REVALORI", "KILCAFE")
movCapital <- daportes.DBF[, vars.movCapital]
f.movCapital <- daportes.DBF[, vars.movCapital]

# Seleccionar datos de maportes
vars.maportes  <- c("PERIODO", "FECHA", "CAPITAL", "SOLIDARI")
# Seleccionar datos 71-93 y 95 de 1 de enero
maportes.71a93_96 <- maportes.DBF[maportes.DBF$PERIODO <= 1996 &
                                    grepl("*-01-01", maportes.DBF$FECHA), 
                                  vars.maportes]
# Retrasar el período un año (la cuota es al 31 de diciembre de cada año)
maportes.71a93_96$PERIODO <- maportes.71a93_96$PERIODO - 1
# Seleccionar datos de 1994 a 2017
maportes.94a17 <- maportes.DBF[maportes.DBF$PERIODO >= 1994 & 
                                    grepl("*-12-31", maportes.DBF$FECHA), 
                               vars.maportes]
# Eliminar duplicados de 2013 y 2015 (siempre dejar el mayor)
maportes.94a17 <- maportes.94a17[order(maportes.94a17$PERIODO,
                                       -maportes.94a17$CAPITAL), ]
maportes.94a17 <- maportes.94a17[!duplicated(maportes.94a17$PERIODO), ]

# Consolidar aportes
aportes <- rbind(maportes.71a93_96, maportes.94a17)

# Agregar cuota a f.movCapital
f.movCapital <- left_join(f.movCapital,
                          select(aportes, -FECHA), by = "PERIODO")
# Remplazar valores en blanco por 0
f.movCapital$CAPITAL <- ifelse(is.na(f.movCapital$CAPITAL),
                               0, f.movCapital$CAPITAL)
f.movCapital$SOLIDARI <- ifelse(is.na(f.movCapital$SOLIDARI),
                               0, f.movCapital$SOLIDARI)
f.movCapital$CAPANUAL <- ifelse(is.na(f.movCapital$CAPANUAL),
                               0, f.movCapital$CAPANUAL)
f.movCapital$SOLANUAL <- ifelse(is.na(f.movCapital$SOLANUAL),
                               0, f.movCapital$SOLANUAL)
f.movCapital$CAPCAFE <- ifelse(is.na(f.movCapital$CAPCAFE),
                               0, f.movCapital$CAPCAFE)
f.movCapital$SOLCAFE <- ifelse(is.na(f.movCapital$SOLCAFE),
                               0, f.movCapital$SOLCAFE)
# Calcular debe
debe <- (f.movCapital$CAPITAL + f.movCapital$SOLIDARI) - 
     (f.movCapital$CAPANUAL + f.movCapital$SOLANUAL + 
           f.movCapital$CAPCAFE + f.movCapital$SOLCAFE)
# Si debe es negativo, 0
debe <- ifelse(debe < 0, 0, debe)
# Agregar debe a f.movCapital
f.movCapital <- data.frame(f.movCapital, debe)
# Borrar debe si es de 1993, 1996 o año actual
anio.actual <- as.numeric(substr(Sys.Date(), 1, 4))
f.movCapital$debe <- ifelse(f.movCapital$PERIODO < 2000 |
                                 f.movCapital$PERIODO == anio.actual, NA,
                            f.movCapital$debe)
# Calcular y agregar aportes
f.movCapital <- data.frame(f.movCapital,
                           aportes = rowSums(select(f.movCapital, CAPINGRE,
                                                      CAPANUAL, DESCPTMO,
                                                    CAPCAFE), 
                                             na.rm = TRUE))


# Validar con tres casos

# ### Importar valores de las cuotas
# cuota.asamblea <- read.csv("Cuota_Asamblea.csv")
# precios <- select(cuota.asamblea, Anio, Vr..Cuota)
# #Convertir a Kgs
# precios$Vr..Cuota <- precios$Vr..Cuota / 12.5
# 
# ### Valor de la cuota a cada registro
# f.movCapital <- left_join(movCapital, precios, by = c("PERIODO" = "Anio"))
# 
# ### Debe
# debe <- ifelse(is.na(f.movCapital$Vr..Cuota), 0,
#                f.movCapital$Vr..Cuota * 12.5) -
#      ifelse(is.na(f.movCapital$aportes), 0, f.movCapital$aportes)
# 
# debe <- ifelse(debe < 0, 0, debe)
# 
# f.movCapital <- data.frame(f.movCapital, debe)
# 
# ### Eliminar variables que no se van a cargar
# f.movCapital <- select(f.movCapital, -Vr..Cuota)

# ## ESTO PARA QUÉ SIRVE??????????? -> COMENTAR
# library(data.table)
# 
# f.movCapital <- setDT(movCapital)
# colsAgg <- c("CAPINGRE", "CAPANUAL", "DESCPTMO", "CAPCAFE",
#              "REVALORI", "KILCAFE")
# f.movCapital <- f.movCapital[, lapply(.SD, sum),
#                                by=.(CEDULA, PERIODO),
#                                .SDcols = colsAgg]
# f.movCapital <- setDF(f.movCapital)
# 
# 
# debe <- ifelse(80000 - (((f.movCapital$KILCAFE * 6000) * 0.01) * 0.8) > 0,
#                80000 - (((f.movCapital$KILCAFE * 6000) * 0.01) * 0.8),
#                0)
# 
# f.movCapital <- data.frame(f.movCapital, debe)


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

# ESTA INFORMACIÓN NO SE INCLUIRÁ EN EL REPORTE

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
f.totales <- full_join(f.totales, t.intPend, by = "CEDULA")
f.totales <- full_join(f.totales, t.intPendCast, by = "CEDULA")


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
f.totales <- full_join(f.totales, t.costJud, by = "CEDULA")
f.totales <- full_join(f.totales, t.costJudCast, by = "CEDULA")

 
## Almacén Vencido

# ELIMINAR EL 1 SOBRANTE DE LAS CÉDULAS

## Crear vector con todas las cédulas de f.totales y de SF
library(RForcecom)
username <- "admin@andes.org"
password <- "admgf2017#XQWRiDpPU6NzJC9Cmm185FF2"
session <- rforcecom.login(username, password)

cc.sf <- rforcecom.retrieve(session, "gfmAg__Farmer__c",
                            "Documento_identidad__c")
cc.sf <- cc.sf$Documento_identidad__c
cc.sf <- cc.sf[!is.na(cc.sf)]
cc.sf <- as.character(cc.sf)
cc.tot <- as.character(f.totales$CEDULA)

cc.tsf <- c(cc.sf, cc.tot)
cc.tsf <- gsub("[^0-9]", "", cc.tsf)
cc.tsf <- unique(cc.tsf)
cc.tsf <- as.numeric(cc.tsf)

rforcecom.logout(session)

## Crear vector que contiene ccs con todos los caracteres y con todos menos
## el último
cc_1 = as.numeric(substr(as.character(f.almacCafe$CEDULA),
                         1,
                         nchar(f.almacCafe$CEDULA) - 1))
f.almacCafe <- data.frame(f.almacCafe, cc_1)

## Todas las cédulas de f.almacCafe que no encuentre en cc.tsf pero
## que sí encuentre al eliminar el último dígito, eliminar el último
## dígito
for (i in 1:nrow(f.almacCafe)) {
     if (f.almacCafe$CEDULA[i] %in% cc.tsf) {
     } else {
          if (f.almacCafe$cc_1[i] %in% cc.tsf) {
               f.almacCafe$CEDULA[i] <- f.almacCafe$cc_1[i]
          }
     }
}

# # Prueba
# 155222721 %in% f.almacCafe$CEDULA #debería dar FALSE
# 15522272 %in% f.almacCafe$CEDULA #debería dar TRUE
# 155255001 %in% f.almacCafe$CEDULA #debería dar FALSE
# 15525500 %in% f.almacCafe$CEDULA #debería dar TRUE
# 571227 %in% f.almacCafe$CEDULA #debería dar TRUE

f.almacCafe <- select(f.almacCafe, -cc_1)


# AGREGAR POR CÉDULA
t.almVencido <- aggregate(x = select(f.almacCafe[f.almacCafe$estado.almacCafe == "Vencida",],
                                     VALOR),
                          by = select(f.almacCafe[f.almacCafe$estado.almacCafe == "Vencida",],
                                      CEDULA),
                          FUN = sum,
                          na.rm = TRUE)
names(t.almVencido)[2] <- "almac.vencida"

f.totales <- full_join(f.totales, t.almVencido, by = "CEDULA")
 

## Almacén Vigente
t.almVigente <- aggregate(x = select(f.almacCafe[f.almacCafe$estado.almacCafe == "Vigente",],
                                     VALOR),
                          by = select(f.almacCafe[f.almacCafe$estado.almacCafe == "Vigente",],
                                      CEDULA),
                          FUN = sum,
                          na.rm = TRUE)
names(t.almVigente)[2] <- "almac.vigente"
f.totales <- full_join(f.totales, t.almVigente, by = "CEDULA")

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

f.totales <- full_join(f.totales, debeAportes, by = "CEDULA")

## Total deuda
t.deuda <- rowSums(select(f.totales, cartera, intPend, intPendCast,
                          costJud, costJudCast, almacen, debe), na.rm = TRUE)
f.totales <- data.frame(f.totales, t.deuda)

## Disponible aportes
t.aportes <- aggregate(x = select(f.movCapital, aportes), 
                       by = select(f.movCapital, CEDULA),
                       FUN = sum, na.rm = TRUE)

t.aportes$aportes <- t.aportes$aportes * 5
names(t.aportes)[2] <- "disponibleAportes"

f.totales <- full_join(f.totales, t.aportes, by = "CEDULA")

## Disponible ingresos
anio.actual <- as.integer(format(Sys.Date(), "%Y"))
anio.anterior <- anio.actual - 1
# Calcular valor total de cuota
Vr..Cuota <- rowSums(select(aportes, CAPITAL, SOLIDARI), na.rm = TRUE)
aportes <- data.frame(aportes, Vr..Cuota)

precios <- select(aportes, PERIODO, Vr..Cuota)
#Convertir a Kgs
precios$Vr..Cuota <- precios$Vr..Cuota / 12.5
# Seleccionar precio año anterior
precio.anio.ant <- precios$Vr..Cuota[precios$PERIODO == anio.anterior]

movCapital.anio.ant <- f.movCapital[f.movCapital$PERIODO == anio.anterior, ]

t.disp.ingresos <- (movCapital.anio.ant$KILCAFE * precio.anio.ant) / 2
movCapital.anio.ant <- data.frame(movCapital.anio.ant,
                                  t.disp.ingresos)

f.totales <- full_join(f.totales, 
                       select(movCapital.anio.ant, CEDULA, t.disp.ingresos),
                       by = "CEDULA")


## Saldo obligaciones actuales: suma de todos los créditos vigentes

# Agregar saldos por cédula
cred.vigentes <- f.RelCartAsoc[f.RelCartAsoc$BORRADOLOG != 1, ]
t_oblig.actuales <- aggregate(x = select(cred.vigentes, vr.actual),
                              by = select(cred.vigentes, CEDULA),
                              FUN = sum, na.rm = TRUE)
# Adicionar a totales
names(t_oblig.actuales)[2] <- "saldo.oblig.actuales"
f.totales <- full_join(f.totales, t_oblig.actuales, by = "CEDULA")


# Disponible actual por aportes 
# = disp.aportes - saldoObligActuales (solo saldo, no intereses)
f.totales <- data.frame(f.totales,
                        disp.actual.aportes = 
                             ifelse(is.na(f.totales$disponibleAportes), 0,
                                    f.totales$disponibleAportes) -
                             ifelse(is.na(f.totales$saldo.oblig.actuales), 0,
                                    f.totales$saldo.oblig.actuales))
                              

# Disponible actual por ingresos 
# = disp.ingresos - saldoObligActuales
f.totales <- data.frame(f.totales,
                        disp.actual.ingresos = 
                             ifelse(is.na(f.totales$t.disp.ingresos), 0,
                                    f.totales$t.disp.ingresos) -
                             ifelse(is.na(f.totales$saldo.oblig.actuales), 0,
                                    f.totales$saldo.oblig.actuales))


## Revalorización
t.revalori <- data.frame(select(o0.vrCupo, CEDULA),
                        revalori = o0.vrCupo[, 6])
f.totales <- full_join(f.totales, t.revalori, by = "CEDULA")

## Capital
t.capital <- data.frame(select(o0.vrCupo, CEDULA),
                        capital = rowSums(o0.vrCupo[, 2:6], na.rm = TRUE))

f.totales <- full_join(f.totales, t.capital, by = "CEDULA")



# 7. BORRAR DATOS EXISTENTES EN SALESFORCE --------------------------------

library(RForcecom)
username <- "admin@andes.org"
password <- "admgf2017#XQWRiDpPU6NzJC9Cmm185FF2"
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

# Suspender la ejecución del script por 5 minutos mientras se borran
# registros existentes de créditos
Sys.sleep(time = 60 * 10)

# 8. CREAR PRODUCTORES NUEVOS EN SALESFORCE -------------------------------

# Flow https://taroworks-8629.cloudforce.com/servlet/servlet.Integration?ic=1&linkToken=VmpFPSxNakF4Tnkwd09TMHdNbFF4TlRvMU5qb3pPQzQzTlRGYSx2OVJrSzJTM1pFN09GdEx6SGp6Q0pILFlXWmtNR0po&lid=01r36000000SYy4
# Información de mapeo de variables en documento "Campos para creacion
# de productores"

## Descargar c?dulas y IDs de objeto farmer
objectName <- "gfmAg__Farmer__c"
fields <- c("Id", "Documento_identidad__c")
productores.sf <- rforcecom.retrieve(session, objectName, fields)
names(productores.sf)[2] <- "CEDASOCIAD"

## Identificar productores activos en masociados que no est?n en SF (farmers)

# Seleccionar variables de masociado a cargar en Salesforce y productores
# activos
vars.masociado <- c("CEDASOCIAD", "NOMBRE1", "NOMBRE2",
                    "APELLIDO1", "APELLIDO2", "CELULAR")
asoc.activos <- masociado.DBF[is.na(masociado.DBF$FECHARET),
                                   vars.masociado]
# Crear variable de nombres y apellidos pegando NOMBRE1 con NOMBRE2 y
# APELLIDO1 con APELLIDO2
asoc.activos <- data.frame(asoc.activos,
                           Nombres = paste(asoc.activos$NOMBRE1,
                                           asoc.activos$NOMBRE2,
                                           sep = " "),
                           Apellidos = paste(asoc.activos$APELLIDO1,
                                             asoc.activos$APELLIDO2,
                                             sep = " "))
# Eliminar NAs creados por paste
asoc.activos$Nombres <- gsub(" NA|NA ", "", asoc.activos$Nombres)
asoc.activos$Apellidos <- gsub(" NA", "", asoc.activos$Apellidos)
# Eliminar nombres y apellidos
asoc.activos <- select(asoc.activos, CEDASOCIAD, Nombres, Apellidos, CELULAR)

# Identificar productores activos que no están en SF
asoc.activos$CEDASOCIAD <- as.character(asoc.activos$CEDASOCIAD)
productores.sf$CEDASOCIAD <- as.character(productores.sf$CEDASOCIAD)
asoc.activos.nosf <- anti_join(asoc.activos, productores.sf,
                               by = "CEDASOCIAD")
# Eliminar cédulas = 0
asoc.activos.nosf <- asoc.activos.nosf[asoc.activos.nosf$CEDASOCIAD != 0, ]
# Eliminar duplicados
asoc.activos.nosf <- asoc.activos.nosf[!duplicated(asoc.activos.nosf$CEDASOCIAD), ] 


# CARGAR CONTACTS

# Cambiar nombres por API names
names(asoc.activos.nosf) <- c("gfsurveys__mobilesurveys_Id__c", "FirstName",
                              "LastName", "MobilePhone")
# Crear trabajo para cargar Persons
job_info <- rforcecom.createBulkJob(session,
                                    operation='insert',
                                    object='Contact')
# Cargar datos
batches_info <- rforcecom.createBulkBatch(session,
                                          jobId=job_info$id,
                                          asoc.activos.nosf,
                                          multiBatch = TRUE,
                                          batchSize=500)
# # Ver estado de carga
# batches_status <- lapply(batches_info,
#                          FUN=function(x){
#                               rforcecom.checkBatchStatus(session,
#                                                          jobId=x$jobId,
#                                                          batchId=x$id)
#                          })
# status <- c()
# for(i in 1:length(batches_status)) {
#      status[i] <- batches_status[[i]]$state
# }
# status
# # Ver detalles de carga
# batches_detail <- lapply(batches_info,
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session,
#                                                         jobId=x$jobId,
#                                                         batchId=x$id)
#                          })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)

## Crear persons

# Descargar registros de contacts de nuevos asociados (es posible volerlo
# más eficiente utilizando un solo query con condición IN?)
for(i in 1:nrow(asoc.activos.nosf)) {
     query <- paste("SELECT Id, gfsurveys__mobilesurveys_Id__c ",
                    "FROM Contact WHERE gfsurveys__mobilesurveys_Id__c = ",
                    paste("\'", 
                          asoc.activos.nosf$gfsurveys__mobilesurveys_Id__c[i],
                          "\'", sep = ""))
     ifelse(i == 1,
            contactos.nuevos <- rforcecom.query(session, query),
            contactos.nuevos <- rbind(contactos.nuevos,
                                      rforcecom.query(session, query)))
}

# Seleccionar datos a cargar en Person (incluido el campo de relación
# a contact) - Mejora futura: cargar vereda, corregimiento y munic.
vars.persons.cargar <- c("CEDASOCIAD", "SEXO", "DIRECCION", "TELEFONO")
persons.cargar <- masociado.DBF[, vars.persons.cargar]
# Renombrar categorías de género
persons.cargar$SEXO <- ifelse(persons.cargar$SEXO == 1, "Masculino",
                              ifelse(persons.cargar$SEXO == 2, "Femenino",
                                     NA))
# Eliminar ccs duplicadas
persons.cargar = persons.cargar[!duplicated(persons.cargar$CEDASOCIAD),]
# Variables de cédula tipo character
persons.cargar$CEDASOCIAD <- as.character(persons.cargar$CEDASOCIAD)
contactos.nuevos$gfsurveys__mobilesurveys_Id__c <- 
     as.character(contactos.nuevos$gfsurveys__mobilesurveys_Id__c)
# Agregar variables a cargar a lista de contactos cargados
persons.cargar <- left_join(contactos.nuevos, persons.cargar,
                            by = c("gfsurveys__mobilesurveys_Id__c" = 
                                        "CEDASOCIAD"))
persons.cargar <- select(persons.cargar, -gfsurveys__mobilesurveys_Id__c)
# Cambiar nombres de variables por API names
names(persons.cargar) <- c("gfmAg__Contact__c", "gfmAg__Gender__c",
                           "gfmAg__address__c", "gfmAg__landline__c")

# CARGAR PERSONS
# Crear trabajo para cargar Persons
job_info <- rforcecom.createBulkJob(session,
                                    operation='insert',
                                    object='gfmAg__Person__c')
# Cargar datos
batches_info <- rforcecom.createBulkBatch(session,
                                          jobId=job_info$id,
                                          persons.cargar,
                                          multiBatch = TRUE,
                                          batchSize=500)
# # Ver estado de carga
# batches_status <- lapply(batches_info,
#                          FUN=function(x){
#                               rforcecom.checkBatchStatus(session,
#                                                          jobId=x$jobId,
#                                                          batchId=x$id)
#                          })
# status <- c()
# for(i in 1:length(batches_status)) {
#      status[i] <- batches_status[[i]]$state
# }
# status
# # Ver detalles de carga
# batches_detail <- lapply(batches_info,
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session,
#                                                         jobId=x$jobId,
#                                                         batchId=x$id)
#                          })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)


## Crear farmers
## En contact hay algunos productores que no cargan porque ya están en
## Salesforce como contactos por no ser farmers pero sí family members.
## A estos contactos también se les debe crear un farmer.

# Descargar persons creados (+id del contacto)
for(i in 1:nrow(asoc.activos.nosf)) {
     query <- paste("SELECT Id, gfmAg__Contact__r.gfsurveys__mobilesurveys_Id__c, ",
                    "gfmAg__Contact__r.Id FROM gfmAg__Person__c WHERE " ,
                    "gfmAg__Contact__r.gfsurveys__mobilesurveys_Id__c = ",
                    paste("\'", 
                          asoc.activos.nosf$gfsurveys__mobilesurveys_Id__c[i],
                          "\'", sep = ""))
     ifelse(i == 1,
            persons.nuevos <- rforcecom.query(session, query),
            persons.nuevos <- rbind(persons.nuevos,
                                      rforcecom.query(session, query)))
}

# Cargar datos

# Cambiar nombres
farmers.cargar <- select(persons.nuevos, Id, Contact.Id)
names(farmers.cargar) <- c("gfmAg__Person__c", "gfmAg__Contact__c")

# Crear trabajo para cargar Persons
job_info <- rforcecom.createBulkJob(session,
                                    operation='insert',
                                    object='gfmAg__Farmer__c')
# Cargar datos
batches_info <- rforcecom.createBulkBatch(session,
                                          jobId=job_info$id,
                                          farmers.cargar,
                                          multiBatch = TRUE,
                                          batchSize=500)
# Ver estado de carga
batches_status <- lapply(batches_info,
                         FUN=function(x){
                              rforcecom.checkBatchStatus(session,
                                                         jobId=x$jobId,
                                                         batchId=x$id)
                         })
status <- c()
for(i in 1:length(batches_status)) {
     status[i] <- batches_status[[i]]$state
}
status
# Ver detalles de carga
batches_detail <- lapply(batches_info,
                         FUN=function(x){
                              rforcecom.getBatchDetails(session,
                                                        jobId=x$jobId,
                                                        batchId=x$id)
                         })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)

# 9. CARGAR CRÉDITO TOTAL -------------------------------------------------

# Descargar farmers de Salesforce (cédula y id)
fields <- c("Id", "Documento_identidad__c")
productores.sf <- rforcecom.retrieve(session, "gfmAg__Farmer__c", fields)
productores.sf <- productores.sf[!duplicated(productores.sf$Documento_identidad__c),]

# Agregar ID de Salesforce a f.totales
f.totales$CEDULA <- as.factor(f.totales$CEDULA)
f.totales <- inner_join(productores.sf, f.totales,
                       by = c("Documento_identidad__c" = "CEDULA"))


# Descripción objeto
# desc.credito_total <- rforcecom.getObjectDescription(session, "credito_Total__c")

# Limpiar y renombrar variables f.totales por API names

f.totales <- select(f.totales, -one_of("car.vencida", "car.vigente", "cartera",
                                       "intPend", "intPendCast", "costJud", 
                                       "costJudCast", "t.deuda", "revalori",
                                       "debe"))

names(f.totales) <- c("Farmer__c", "Cedula__c", "Almacen_vencido__c",
                      "Almacen_vigente__c", "Total_almacen__c",
                      "Disponible_aportes__c", "Disponible_ingresos__c", 
                      "Saldo_obligaciones_actuales__c", 
                      "Disponible_actual_por_aportes__c",
                      "Disponible_actual_por_ingresos__c", "Total_Capital__c")

f.totales <- f.totales[order(f.totales$Farmer__c), ]
f.totales <- f.totales[!duplicated(f.totales$Cedula__c), ]

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

# # check on status of each batch
# batches_status <- lapply(batches_info,
#                          FUN=function(x){
#                               rforcecom.checkBatchStatus(session,
#                                                          jobId=x$jobId,
#                                                          batchId=x$id)
#                          })
# # get details on each batch
# batches_detail <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session, 
#                                                         jobId=x$jobId, 
#                                                         batchId=x$id)
#                          })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)

# Suspender ejecución del script mientras se crean todos los registros de
# Crédito total

Sys.sleep(time = 60 * 10)

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

f.almacCafe <- select(f.almacCafe, -one_of(c("CEDULA", "NOMALMACEN")))

names(f.almacCafe) <- c("ALMACEN__c", "REFERENCIA__c", "FECHA__c",
                        "FECVEN__c", "VALOR__c", "Estado_o__c",
                        "credito_Total__c")

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

# Preparar datos
f.disponible$CEDULA <- as.factor(f.disponible$CEDULA)
f.disponible <- inner_join(f.disponible, credito.total,
                          by = c("CEDULA" = "Cedula__c"))

# f.disponible <- select(f.disponible, -one_of(c("CEDULA", "REFERENCIA",
#                                              "NOMALMACEN")))

f.disponible <- select(f.disponible, -CEDULA)

names(f.disponible) <- c("L_nea__c", "Valor_cupo__c", "Valor_cr_ditos__c",
                         "Valor_disponible__c", "credito_Total__c")

f.disponible <- f.disponible[order(f.disponible$credito_Total__c), ]

# Cargar datos

job_info <- rforcecom.createBulkJob(session,
                                    operation='insert',
                                    object='Disponible__c')

batches_info <- rforcecom.createBulkBatch(session,
                                          jobId=job_info$id,
                                          f.disponible,
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

# 13. CARGAR DAPORTES (MOVCAPITAL) ----------------------------------------

# Preparar datos
f.movCapital$CEDULA <- as.factor(f.movCapital$CEDULA)
f.movCapital <- inner_join(f.movCapital, credito.total,
                          by = c("CEDULA" = "Cedula__c"))

f.movCapital <- select(f.movCapital, -one_of(c("CEDULA", "SOLANUAL", 
                                               "SOLCAFE", "CAPITAL",
                                               "SOLIDARI", "aportes",
                                               "CAPANUAL")))

names(f.movCapital) <- c("PERIODO__c", "CAPINGRESO__c", "DESCPTMO__c",
                         "CAPCAFE__c", "REVALORI__c", "KILCAFE__c",
                         "CAPANUAL__c", "credito_Total__c")

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

# batches_status <- lapply(batches_info,
#                          FUN=function(x){
#                               rforcecom.checkBatchStatus(session,
#                                                          jobId=x$jobId,
#                                                          batchId=x$id)
#                          })
# batches_status[[1]]$state 
# batches_detail <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session, 
#                                                         jobId=x$jobId, 
#                                                         batchId=x$id)
#                          })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)

# 14. CARGAR CAR_VIGENTE --------------------------------------------------

# Preparar datos
f.RelCartAsoc$CEDULA <- as.factor(f.RelCartAsoc$CEDULA)
f.RelCartAsoc <- inner_join(f.RelCartAsoc, credito.total,
                           by = c("CEDULA" = "Cedula__c"))

f.RelCartAsoc <- select(f.RelCartAsoc, -one_of(c("CEDULA", "MUNICIPIO",
                                                 "NOAFECTACU")))

names(f.RelCartAsoc) <- c("Tipo_de_cr_dito__c", "CODCREDITO__c", "NUMERO__c",
                          "FECHACRE__c", "FECHAVEN__c", "VRCREDITO__c",
                          "REFINANCIA__c", "Reestructurado__c",
                          "DESCREDITO__c", "VRPAGOS__c", "VRACTUAL__c",
                          "Estado__c", "Borrado_l_gico__c", "credito_Total__c")

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

batches_status <- lapply(batches_info,
                         FUN=function(x){
                              rforcecom.checkBatchStatus(session,
                                                         jobId=x$jobId,
                                                         batchId=x$id)
                         })
batches_status[[1]]$state
# batches_detail <- lapply(batches_info, 
#                          FUN=function(x){
#                               rforcecom.getBatchDetails(session, 
#                                                         jobId=x$jobId, 
#                                                         batchId=x$id)
#                          })

close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)



# 15. PRUEBA DE EJECUCIÓN -------------------------------------------------
# 
# setwd("C:/Users/Ramiro/Desktop/Prueba scheduleR")
# prueba <- as.vector(read.csv("prueba_scheduleR.csv")[, 1])
# 
# prueba[length(prueba) + 1] <- as.character(Sys.time())
# 
# write.csv(prueba, "prueba_scheduleR.csv", row.names = FALSE)
# 
# # Cargar objetos importados (.dbf) guardados para pruebas
# load("C:/Users/Ramiro/rcadavid@grameenfoundation.org/7. Proyectos/Activos/LAC/DelosAndes Cooperativa/Diseno de herramientas/03. ARET/DBF a Salesforce/Tableros-credito-Andes/Datos/29 agosto 2017/data.RData")
