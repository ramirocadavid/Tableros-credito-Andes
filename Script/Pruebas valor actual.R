
# Valor actual negativo ---------------------------------------------------

# Caso 1: Crédito 23360
View(car_vigente.DBF[car_vigente.DBF$NUMERO == 23360, ])
View(car_vigente[car_vigente$NUMERO == 23360, ])
View(car_pagos.DBF[car_pagos.DBF$NUMEROCRE == 23360, ])

# Parece que hay algunos créditos duplicados en car_vigente (se repite más
# de una vez cedula+codcredito+numerocre) por lo tanto, puede haber varios
# registros en car_pagos que se suman varias veces.
# Este caso quede para el final porque no afecta los resultados del tablero
# (si el vr.actual es negativo, no se muestra).
# 
# Solución temporal: todos los vr.actual < 0, 0.







# No utilizar car_pagos sino car_vigente$VRPAGADO -------------------------

p.vars.car_vigente <- c("CEDULA", "CODCREDITO", "NUMERO", "FECHACRE", "FECHAVEN",
                      "VRCREDITO", "REFINANCIA", "REESTRUC", "MUNICIPIO",
                      "VRPAGADO")
p.car.vigente <- car_vigente.DBF[, p.vars.car_vigente]
valor.actual <- p.car.vigente$VRCREDITO - p.car.vigente$VRPAGADO
p.car.vigente <- data.frame(p.car.vigente, valor.actual)

View(p.car.vigente[p.car.vigente$CEDULA == 15522272,])
View(car_vigente.DBF[car_vigente.DBF$CEDULA == 15522272,])

# Resultado: da diferente, sí es necesario utilizar car_pagos.DBF