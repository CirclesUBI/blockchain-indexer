FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
ENV DOTNET_EnableDiagnostics=0
WORKDIR /src
COPY . .
RUN cd CirclesLand.BlockchainIndexer.Server && dotnet restore "CirclesLand.BlockchainIndexer.Server.csproj"
RUN cd CirclesLand.BlockchainIndexer.Server && dotnet build "CirclesLand.BlockchainIndexer.Server.csproj" -c Release -o /app/build

FROM build AS publish
ENV DOTNET_EnableDiagnostics=0
RUN cd CirclesLand.BlockchainIndexer.Server && dotnet publish "CirclesLand.BlockchainIndexer.Server.csproj" -c Release -o /app/publish

FROM base AS final
LABEL org.opencontainers.image.source=https://github.com/circlesland/blockchain-indexer
ENV DOTNET_EnableDiagnostics=0
ENV INDEXER_RPC_GATEWAY_URL ''
ENV INDEXER_CONNECTION_STRING ''
ENV INDEXER_WEBSOCKET_PORT='8675'
ENV ASPNETCORE_hostBuilder__reloadConfigOnChange=false

WORKDIR /app
EXPOSE 8675
COPY --from=publish /app/publish .
RUN chmod +x ./CirclesLand.BlockchainIndexer.Server
ENTRYPOINT ["./CirclesLand.BlockchainIndexer.Server"]
