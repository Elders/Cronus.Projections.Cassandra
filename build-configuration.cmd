@echo off

%FAKE% %NYX% "target=clean" -st
%FAKE% %NYX% "target=RestoreNugetPackages" -st


IF NOT [%1]==[] (set RELEASE_NUGETKEY="%1")

SET SUMMARY="Cronus.Projections.Cassandra"
SET DESCRIPTION="Cronus.Projections.Cassandra"

%FAKE% %NYX% appName=Elders.Cronus.Projections.Cassandra appSummary=%SUMMARY% appDescription=%DESCRIPTION% nugetPackageName=Cronus.Projections.Cassandra nugetkey=%RELEASE_NUGETKEY%