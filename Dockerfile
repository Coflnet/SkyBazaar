FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /build
RUN git clone --depth=1 https://github.com/Coflnet/HypixelSkyblock.git dev
WORKDIR /build/sky
COPY SkyBazaar.csproj SkyBazaar.csproj
RUN dotnet restore
COPY . .
RUN dotnet publish SkyBazaar.csproj -c release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app .
RUN mkdir -p ah/files

ENV ASPNETCORE_URLS=http://+:8000

RUN useradd --create-home --uid 10001 app-user
USER app-user

ENTRYPOINT ["dotnet", "SkyBazaar.dll", "--hostBuilder:reloadConfigOnChange=false"]

VOLUME /data
