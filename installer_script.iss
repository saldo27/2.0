; Script de instalación para GuardiasApp
; Generado con Inno Setup

#define MyAppName "GuardiasApp"
#define MyAppVersion "2.0"
#define MyAppPublisher "Luis Herrera Para"
#define MyAppURL "https://github.com/saldo27/2.0"
#define MyAppExeName "GuardiasApp.exe"
#define SourceDir "C:\Py\v2.0"

[Setup]
; Información de la aplicación
AppId={{A7B8C9D0-E1F2-3456-7890-ABCDEF123456}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
OutputDir={#SourceDir}\installer_output
OutputBaseFilename=GuardiasApp_Setup_v{#MyAppVersion}
SetupIconFile={#SourceDir}\icon.ico
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription:  "{cm:AdditionalIcons}"; Flags: unchecked
Name: "quicklaunchicon"; Description: "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1; Check: not IsAdminInstallMode

[Files]
; Incluir toda la carpeta de distribución
Source: "{#SourceDir}\dist\GuardiasApp\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
; Icono en el menú de inicio
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"

; Icono en el escritorio (opcional)
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks:  desktopicon

[UninstallDelete]
Type: filesandordirs; Name: "{app}\*"
Type: dirifempty; Name: "{app}"

[Run]
; Ejecutar la aplicación después de instalar (opcional)
Filename: "{app}\{#MyAppExeName}"; Description:  "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent