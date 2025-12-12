{ pkgs, ... }:

{
  packages = with pkgs; [
    minio-client
  ];

  languages.rust = {
    enable = true;
    channel = "stable";
  };
}
