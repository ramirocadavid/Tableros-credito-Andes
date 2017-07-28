
# Hay variables de fecha que indiquen cuándo se castigó? SÍ ----------------

View(car_vigente.DBF[order(car_vigente.DBF$CEDULA, car_vigente.DBF$FECHA1),
                     c("CEDULA", "FECHACRE", "FECHAVEN", "FECHA1", 
                       "FECHA2", "FECHA3", "FECHA4")])

## En car_vigente: NO
##
# Variables de fecha
clases <- sapply(car_vigente.DBF, class)
fechas <- clases == "Date"
vars.fechas <- clases[fechas == TRUE]
vars.fechas <- data.frame(vars.fechas)
vars.fechas <- rownames(vars.fechas)

View(car_vigente.DBF[, vars.fechas])


## En car_castigada: SÍ
##
# Variables de fecha
clases <- sapply(car_castigada.DBF, class)

fechas <- clases == "Date"
vars.fechas <- clases[fechas == TRUE]
vars.fechas <- data.frame(vars.fechas)
vars.fechas <- rownames(vars.fechas)

View(car_castigada.DBF[, vars.fechas])



# Créditos castigados están también en vigentes? SÍ-------------------------
# Para agregar la fecha de castigo (cuándo dejaron de ser vigentes)

vigente <- data.frame(NUMERO = car_vigente.DBF$NUMERO,
                      UNO = rep(1, nrow(car_vigente.DBF)))

library(dplyr)
cartera.completa <- left_join(car_castigada.DBF, vigente, by = "NUMERO")
table(is.na(cartera.completa$UNO))
