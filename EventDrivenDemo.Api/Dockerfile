FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY EventDrivenDemo.Shared/EventDrivenDemo.Shared.csproj EventDrivenDemo.Shared/
COPY EventDrivenDemo.Api/EventDrivenDemo.Api.csproj EventDrivenDemo.Api/
RUN dotnet restore EventDrivenDemo.Api/EventDrivenDemo.Api.csproj

COPY EventDrivenDemo.Shared/ EventDrivenDemo.Shared/
COPY EventDrivenDemo.Api/ EventDrivenDemo.Api/
RUN dotnet publish EventDrivenDemo.Api/EventDrivenDemo.Api.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "EventDrivenDemo.Api.dll"]
