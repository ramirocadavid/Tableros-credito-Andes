cc.masociado <- table(masociado.DBF$CEDASOCIAD)
cc.daportes <- table(daportes.DBF$CEDULA)
cc.carVigente <- table(car_vigente.DBF$CEDULA)

nrow(cc.masociado)
nrow(cc.daportes)

nrow(full_join(cc.masociado, cc.daportes))

