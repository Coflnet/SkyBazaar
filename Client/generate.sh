VERSION=0.2.0

docker run --rm -v "${PWD}:/local" --network host -u $(id -u ${USER}):$(id -g ${USER})  openapitools/openapi-generator-cli generate \
-i http://localhost:5011/swagger/v1/swagger.json \
-g csharp-netcore \
-o /local/out --additional-properties=packageName=Coflnet.Sky.Bazaar.Client,packageVersion=$VERSION,licenseId=MIT

cd out
sed -i 's/GIT_USER_ID/Coflnet/g' src/Coflnet.Sky.Bazaar.Client/Coflnet.Sky.Bazaar.Client.csproj
sed -i 's/GIT_REPO_ID/SkyFlipTracker/g' src/Coflnet.Sky.Bazaar.Client/Coflnet.Sky.Bazaar.Client.csproj
sed -i 's/>OpenAPI/>Coflnet/g' src/Coflnet.Sky.Bazaar.Client/Coflnet.Sky.Bazaar.Client.csproj

dotnet pack
cp src/Coflnet.Sky.Bazaar.Client/bin/Debug/Coflnet.Sky.Bazaar.Client.*.nupkg ..
