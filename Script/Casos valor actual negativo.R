View(f.RelCartAsoc[f.RelCartAsoc$vr.actual < 0, ])

f.RelCartAsoc[f.RelCartAsoc$NUMERO == 2069, c(1:4, 11)]
# CEDULA CODCREDITO NUMERO   FECHACRE vr.pagos
# 4     70413700          2   2069 2001-11-24  2500000
# 21556 70413700          2   2069 2001-11-24        0

car_pagos.DBF[car_pagos.DBF$NUMEROCRE == 2069, c(1, 2, 4, 5, 11)]
# CEDULA CODCREDITO NUMEROCRE      FECHA CREDITO
# 1311    572420          6      2069 2002-03-17 4980003
# 15033 15532889          2      2069 2001-12-23  500000
# 70467  3376097          2      2069 2010-01-02 2000000
# 78665 70413700          2      2069 2012-10-06 2500000

# No debería haber dos créditos con el mismo código:
creditos <- as.data.frame(table(car_vigente.DBF$NUMERO))
nrow(creditos[creditos$Freq > 1, ])
nrow(creditos[creditos$Freq > 1, ]) / nrow(creditos)
# Números de crédito con más de un registro en car_vigente: 2435 (13%)

# Distribución
# Repeticiones  1      2       3     4 
# Registros    16323  2387    46     2
