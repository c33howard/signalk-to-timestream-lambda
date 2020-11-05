all:
	zip -r function.zip --exclude='.git/*' --exclude=function.zip .
	aws lambda update-function-code --function-name signalk-to-timestream --zip-file fileb://function.zip

.PHONY: all
