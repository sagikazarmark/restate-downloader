{ pkgs, ... }:

{
  dotenv.enable = true;

  packages = with pkgs; [
    minio-client
  ];

  languages.rust = {
    enable = true;
    channel = "stable";
  };
}
