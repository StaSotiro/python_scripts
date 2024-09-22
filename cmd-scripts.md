Please find below the executed commands for connecting to the deployed VM

1. ssh -i ~/.ssh/id_rsa stasotiro@52.174.54.99 (prompted for the password I provided during creation)
2. curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
3. az storage blob list --container-name stavros-sotiropoulos --account-name=dataengineerv1  --account-key ACCOUNT_KEY
4. az storage blob download --container-name stavros-sotiropoulos --name Stavros-Sotiropoulos.csv --file /home/stasotiro/result-Stavros.csv --account-name=dataengineerv1  --account-key ACCOUNT_KEY
