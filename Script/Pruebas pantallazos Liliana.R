# Directorio antiguo
setwd("C:/Users/Ramiro/rcadavid@grameenfoundation.org/7. Proyectos/Activos/DelosAndes Cooperativa/Diseno de herramientas/03. ARET/DBF a Salesforce/Archivos antiguos/Datos")
#Directorio actualizado
setwd("C:/Users/Ramiro/rcadavid@grameenfoundation.org/7. Proyectos/Activos/DelosAndes Cooperativa/Diseno de herramientas/03. ARET/DBF a Salesforce/Tableros-credito-Andes/Datos")
#Cédula de prueba
ccs <- c(3417689, 21461147, 98630518, 3419167)
cc <- ccs[1]

# Relación cartera asociado
View(f.RelCartAsoc[f.RelCartAsoc$CEDULA == cc & f.RelCartAsoc$estado == "Vigente", ])
View(car_vigente.DBF[car_vigente.DBF$CEDULA == cc, ])
View(car_pagos.DBF[car_pagos.DBF$CEDULA == cc, ])
View(car_pagos.DBF[car_pagos.DBF$CEDULA == cc & 
                        car_pagos.DBF$NUMEROCRE == 36158, ])

# Créditos almacenes del café
View(f.almacCafe[f.almacCafe$CEDULA == cc, ])
View(factalma.DBF[factalma.DBF$CEDULA == cc, ])

# Movimiento detallado capital del asociado
f.movCapital[f.movCapital$CEDULA == cc, ]
View(daportes.DBF[daportes.DBF$CEDULA == cc, ])

# Observaciones
f.observaciones[f.observaciones$CEDULA == cc, ]

# Disponible
f.disponible[f.disponible$CEDULA == cc, ]
## Cupo - ordinario (3*[CAPINGRE + CAPANUAL + DESCPTMO + CAPCAFE + RECVALORI])
View(o0.vrCupo[o0.vrCupo$CEDULA == cc, ])
## Crédito - ordinario (sum vr.actual)
View(car_vigente_no6[car_vigente_no6$CEDULA == cc, ])

# Totales
View(f.totales[f.totales$CEDULA == cc, ])
