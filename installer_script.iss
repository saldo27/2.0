; Script de instalación para GuardiasApp
; Generado con Inno Setup

#define MyAppName "GuardiasApp"
#define MyAppVersion "2.0"
#define MyAppPublisher "Luis Herrera Para"
#define MyAppURL "https://github.com/saldo27/2.0"
#define MyAppExeName "GuardiasApp.exe"

[Setup]
; Información de la aplicación
AppId={{TU-GUID-UNICO-AQUI}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
SetupIconFile=icon.ico           ; ← Icono del instalador
UninstallDisplayIcon={app}\GuardiasApp.exe  ; ← Icono del desinstalador
LicenseFile=LICENSE. txt
InfoBeforeFile=README.txt
OutputDir=installer_output
OutputBaseFilename=GuardiasApp_Setup_v{#MyAppVersion}
SetupIconFile=icon.ico
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm: CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name:  "quicklaunchicon"; Description:  "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1; Check: not IsAdminInstallMode

[Files]
; Incluir toda la carpeta de distribución
Source: "dist\GuardiasApp\*"; DestDir: "{app}"; Flags:  ignoreversion recursesubdirs createallsubdirs

[Icons]
; Icono en el menú de inicio
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name:  "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"

; Icono en el escritorio (opcional)
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

; Icono en inicio rápido (opcional)
Name: "{userappdata}\Microsoft\Internet Explorer\Quick Launch\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: quicklaunchicon

[Run]
; Ejecutar la aplicación después de instalar (opcional)
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

