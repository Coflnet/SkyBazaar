FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /build
RUN git clone --depth=1 https://github.com/Coflnet/HypixelSkyblock.git dev
WORKDIR /build/sky
COPY SkyBazaar.csproj SkyBazaar.csproj
RUN dotnet restore
COPY . .
RUN dotnet test SkyBazaar.csproj
RUN dotnet publish SkyBazaar.csproj -c release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app .
RUN mkdir -p ah/files

ENV ASPNETCORE_URLS=http://+:8000

# apt in the base image has CVE-2026-109518 (HIGH); upgrade it until the base image ships the fix.
RUN apt-get update \
    && apt-get install -y --only-upgrade --no-install-recommends apt libapt-pkg6.0t64 \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 app-user
USER app-user

ENTRYPOINT ["dotnet", "SkyBazaar.dll", "--hostBuilder:reloadConfigOnChange=false"]

VOLUME /data
