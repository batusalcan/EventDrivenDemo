FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY EventDrivenDemo.Shared/EventDrivenDemo.Shared.csproj EventDrivenDemo.Shared/
COPY EventDrivenDemo.InvoiceApi/EventDrivenDemo.InvoiceApi.csproj EventDrivenDemo.InvoiceApi/
RUN dotnet restore EventDrivenDemo.InvoiceApi/EventDrivenDemo.InvoiceApi.csproj

COPY EventDrivenDemo.Shared/ EventDrivenDemo.Shared/
COPY EventDrivenDemo.InvoiceApi/ EventDrivenDemo.InvoiceApi/
RUN dotnet publish EventDrivenDemo.InvoiceApi/EventDrivenDemo.InvoiceApi.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "EventDrivenDemo.InvoiceApi.dll"]
