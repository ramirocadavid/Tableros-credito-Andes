
# DIRECTORIOS Y TABLERO ---------------------------------------------------

# Inicial: 10/07/2017
setwd("Datos")
# Segunda prueba: 18/07/2017
setwd("Datos/DBF Andes 18 Julio 2017")
# Tercera prueba: 27/07/2017
setwd("Datos/DBF Andes 27 Julio 2017")

# Tercera prueba (truncando car_pagos al 01/08/2017)
setwd("Datos/DBF Andes 27-07 o 01-08 2017")
## Truncar datos
car_pagos.DBF <- car_pagos.DBF[car_pagos.DBF$FECHA < "2017-08-01", ]

# Cuarta prueba  (31 de agosto)
setwd("Datos/Pruebas 31 agosto 2017")


# Tableros
# https://taroworks-8629--c.na30.visual.force.com/apex/Credito_EstadoAsociado?sfdc.tabName=01r36000000qs7a



# CEDULAS DE PRUEBA -------------------------------------------------------

#Cédula de prueba
ccs <- c(3417689, 98630518, 3419167, 21461147) #pantallazos 1
ccs <- c(21551811, 15526081, 21463508) #pantallazos 2
ccs <- c(10195996, 15526383) #?
ccs <- c(15522272, 15525500, 571227) #pantallazos 3
ccs <- c(3417689, 98630518, 3419167, 21461147, 21551811, 15526081, 21463508,
         10195996, 15526383, 15522272, 15525500, 571227, 1007026, 43284018,
         15530748, 3374770) #pantallazos 4

# Cédula a probar
cc <- ccs[1]
cc <- 15530748

# PRUEBAS -----------------------------------------------------------------

# CRÉDITOS

## car_vigente
View(car_vigente.DBF[car_vigente.DBF$CEDULA == cc, ])

## RelCartAsoc
View(f.RelCartAsoc[f.RelCartAsoc$CEDULA == cc,])

f.RelCartAsoc[f.RelCartAsoc$CEDULA == cc,
              c("CEDULA", "vr.actual")]

View(f.RelCartAsoc[f.RelCartAsoc$CEDULA == cc &
                        f.RelCartAsoc$vr.actual > 0, ])

View(f.RelCartAsoc[f.RelCartAsoc$NUMERO == 32325, ])

View(f.RelCartAsoc[f.RelCartAsoc$CEDULA == cc & (
                        f.RelCartAsoc$NUMERO == 27944|
                        f.RelCartAsoc$NUMERO == 31806|
                        f.RelCartAsoc$NUMERO == 31812), ])

## car_pagos
View(car_pagos.DBF[car_pagos.DBF$CEDULA == cc,
                   c("CEDULA", "CODCREDITO", "MUNICIPIO", "NUMEROCRE", "FECHA",
                     "FECHACRE", "FECHAVEN", "DIASCRED", "DIASPLAZ",
                     "DIASMORA", "CREDITO")])

View(car_pagos.DBF[car_pagos.DBF$NUMEROCRE == 32325, ])

View(car_pagos.DBF[car_pagos.DBF$CEDULA == cc & (
                        car_pagos.DBF$NUMEROCRE == 27944|
                        car_pagos.DBF$NUMEROCRE == 31806|
                        car_pagos.DBF$NUMEROCRE == 31812), ])


# ALMACENES DE CAFÉ

View(f.almacCafe[f.almacCafe$CEDULA == cc, ])
View(factalma.DBF[factalma.DBF$CEDULA == cc, ])


# MOVIMIENTO DE CAPITAL

View(f.movCapital[f.movCapital$CEDULA == cc,])
f.movCapital[f.movCapital$CEDULA == cc, c("CEDULA", "PERIODO", "CAPCAFE",
                                          "KILCAFE")]
View(daportes.DBF[daportes.DBF$CEDULA == cc, ])


# OBSERVACIONES
f.observaciones[f.observaciones$CEDULA == cc, ]


# DISPONIBLE
f.disponible[f.disponible$CEDULA == cc, ]

## Cupo - ordinario (3*[CAPINGRE + CAPANUAL + DESCPTMO + CAPCAFE + RECVALORI])
View(o0.vrCupo[o0.vrCupo$CEDULA == cc, ])

## Crédito - ordinario (sum vr.actual)
View(car_vigente_no6[car_vigente_no6$CEDULA == cc, ])


# TOTALES

View(f.totales[f.totales$CEDULA == cc, ])
f.totales[f.totales$CEDULA == cc, "disp.actual.aportes"]
View(f.totales[f.totales$CEDULA == cc,
               c("CEDULA", "disponibleAportes", "t.disp.ingresos",
                 "saldo.oblig.actuales", "disp.actual.aportes",
                 "disp.actual.ingresos")])



