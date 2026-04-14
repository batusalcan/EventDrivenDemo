FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY EventDrivenDemo.Shared/EventDrivenDemo.Shared.csproj EventDrivenDemo.Shared/
COPY EventDrivenDemo.NotificationApi/EventDrivenDemo.NotificationApi.csproj EventDrivenDemo.NotificationApi/
RUN dotnet restore EventDrivenDemo.NotificationApi/EventDrivenDemo.NotificationApi.csproj

COPY EventDrivenDemo.Shared/ EventDrivenDemo.Shared/
COPY EventDrivenDemo.NotificationApi/ EventDrivenDemo.NotificationApi/
RUN dotnet publish EventDrivenDemo.NotificationApi/EventDrivenDemo.NotificationApi.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "EventDrivenDemo.NotificationApi.dll"]
