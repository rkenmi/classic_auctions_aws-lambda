#Classic Auctions Lua Scanner Lambda Service

## How to use
1. Add dependencies in the `package` directory, using pip.

Ubuntu:
```
pip3 install --target ./package aws-requests-auth --system
```

2. Zip up the packages

```
cd packages
zip -r9 ${OLDPWD}/function.zip .
```

3. Zip up the lambda function
```
cd ${OLDPWD}
zip -g function.zip lambda_function.py
```

4. Deploy
```
sudo aws lambda update-function-code --function-name LuaScannerService --zip-file fileb://function.zip
```
