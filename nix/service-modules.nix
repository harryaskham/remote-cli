{
  packageFor,
  appName,
  displayName ? appName,
  binary ? appName,
  defaultBind,
  description ? "${displayName} cache collector",
  darwinCacheBase ? "Library/Caches",
}:
let
  commonOptions =
    { lib, pkgs, home, systemd ? false, cacheBase ? ".cache" }:
    let
      defaultConfig = if systemd then "%h/.config/${appName}/config.yaml" else "${home}/.config/${appName}/config.yaml";
      defaultCache = if systemd then "%h/.cache/${appName}/state.json" else "${home}/${cacheBase}/${appName}/state.json";
    in
    {
      enable = lib.mkEnableOption description;
      package = lib.mkOption {
        type = lib.types.package;
        default = packageFor pkgs;
        description = "${displayName} package whose daemon owns upstream refreshes.";
      };
      bind = lib.mkOption {
        type = lib.types.str;
        default = defaultBind;
        description = "Authenticated snapshot/SSE bind address; empty disables TCP when unixSocket is set.";
      };
      unixSocket = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Optional owner-local Unix socket serving the same authenticated snapshot/SSE protocol.";
      };
      configFile = lib.mkOption {
        type = lib.types.str;
        default = defaultConfig;
        description = "${displayName} YAML config path.";
      };
      cacheFile = lib.mkOption {
        type = lib.types.str;
        default = defaultCache;
        description = "Atomic ${displayName} cache path.";
      };
      tokenFile = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Existing owner-only bearer token. Null lets the daemon create one beside config mode 0600.";
      };
      extraArgs = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [ ];
        description = "Extra arguments appended to `${binary} daemon`.";
      };
    };

  mkArgs = lib: cfg:
    [
      "${lib.getExe' cfg.package binary}"
      "--config"
      cfg.configFile
      "--cache"
      cfg.cacheFile
      "daemon"
      "--bind"
      cfg.bind
    ]
    ++ lib.optionals (cfg.unixSocket != null) [
      "--unix-socket"
      cfg.unixSocket
    ]
    ++ lib.optionals (cfg.tokenFile != null) [
      "--token-file"
      cfg.tokenFile
    ]
    ++ cfg.extraArgs;
in
{
  nixos =
    { config, lib, pkgs, ... }:
    let
      cfg = config.services.${appName};
      args = mkArgs lib cfg;
    in
    {
      options.services.${appName} = commonOptions {
        inherit lib pkgs;
        home = "%h";
        systemd = true;
      };
      config = lib.mkIf cfg.enable {
        environment.systemPackages = [ cfg.package ];
        systemd.user.services."${appName}-daemon" = {
          inherit description;
          wantedBy = [ "default.target" ];
          unitConfig.ConditionUser = "!@system";
          serviceConfig = {
            ExecStart = lib.escapeShellArgs args;
            Restart = "on-failure";
            RestartSec = 5;
            RestartSteps = 5;
            RestartMaxDelaySec = 300;
          };
        };
      };
    };

  darwin =
    { config, lib, pkgs, ... }:
    let
      cfg = config.services.${appName};
      primaryUser = config.system.primaryUser or "harryaskham";
      home = config.users.users.${primaryUser}.home or "/Users/${primaryUser}";
      args = mkArgs lib cfg;
    in
    {
      options.services.${appName} = commonOptions {
        inherit lib pkgs home;
        cacheBase = darwinCacheBase;
      };
      config = lib.mkIf cfg.enable {
        environment.systemPackages = [ cfg.package ];
        launchd.user.agents."${appName}-daemon" = {
          command = lib.escapeShellArgs args;
          serviceConfig = {
            KeepAlive = true;
            RunAtLoad = true;
            ProcessType = "Background";
            ThrottleInterval = 5;
            StandardOutPath = "${home}/Library/Logs/${appName}-daemon.log";
            StandardErrorPath = "${home}/Library/Logs/${appName}-daemon.err.log";
          };
        };
      };
    };

  nixOnDroid =
    { config, lib, pkgs, ... }:
    let
      cfg = config.services.${appName};
      args = mkArgs lib cfg;
    in
    {
      options.services.${appName} = (commonOptions {
        inherit lib pkgs;
        home = cfg.homeDir;
      }) // {
        homeDir = lib.mkOption {
          type = lib.types.str;
          default = "/home/nix-on-droid";
          description = "Home directory exported to the ${displayName} daemon.";
        };
      };
      config = lib.mkIf cfg.enable {
        environment.packages = [ cfg.package ];
        supervisord.programs."${appName}-daemon" = {
          command = lib.escapeShellArgs args;
          directory = cfg.homeDir;
          path = [ cfg.package ];
          autostart = true;
          autorestart = true;
          startsecs = 2;
          environment = {
            HOME = cfg.homeDir;
            XDG_CONFIG_HOME = "${cfg.homeDir}/.config";
            XDG_CACHE_HOME = "${cfg.homeDir}/.cache";
          };
        };
      };
    };
}
