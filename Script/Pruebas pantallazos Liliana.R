cc <- 3373912

# Relación cartera asociado
View(f.RelCartAsoc[f.RelCartAsoc$CEDULA == cc, ])

# Créditos almacenes del café
View(f.almacCafe[f.almacCafe$CEDULA == cc, ])

# Movimiento detallado capital del asociado
View(f.movCapital[f.movCapital$CEDULA == cc, ])

# Observaciones
View(f.observaciones[f.observaciones$CEDULA == cc, ])

# Disponible
View(f.disponible[f.disponible$CEDULA == cc, ])

# Totales
View(f.totales[f.totales$CEDULA == cc, ])
