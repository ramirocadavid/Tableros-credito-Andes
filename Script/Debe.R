# Variables en daportes
names(daportes.DBF)

# ¿Hay un registro único por productor y año?
validacion <- data.frame(codUnico =  paste(daportes.DBF$CEDULA,
                                           daportes.DBF$PERIODO),
                            daportes.DBF)
validRegUnico <- as.data.frame(table(validacion$codUnico))
table(validRegUnico$Freq)

# 113 registros con múltiples ocurrencias:
nrow(validRegUnico[validRegUnico$Freq > 1, ]) 
mult <- validRegUnico[validRegUnico$Freq > 1, 1]

View(daportes.DBF[validacion$codUnico == mult[1], ])
# Aunque hay 113 registros donde se repiten códigos únicos (cc + Periodo,
# pareciera como si cada registro fuera el consolidado anual, por lo que
# sí se debe suponer un precio para calcular los aportes)
