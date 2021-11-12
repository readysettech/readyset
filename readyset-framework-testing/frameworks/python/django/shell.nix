{ pkgs ? import <nixpkgs> {
  overlays = [(self: super: {
    postgresql = self.enableDebugging super.postgresql;
  })];
} }:

with pkgs;

mkShell {
  buildInputs = [
    (python39.withPackages (p: with p; [
      asgiref
      django
      pytz
      sqlparse
      (psycopg2.overridePythonAttrs (_: { dontStrip = true; }))
    ]))

    (writeShellScriptBin "run_test" ''
      python ${./src}/manage.py makemigrations
      python ${./src}/manage.py migrate
      python ${./src}/manage.py shell < ${./src}/test.py
    '')
  ];

  DATABASE_ENGINE = "django.db.backends.postgresql";
  RS_HOST = "localhost";
  RS_PORT = "5433";
  RS_USERNAME = "postgres";
  RS_PASSWORD = "whatever";
  RS_DATABASE = "idontcare";
  RS_NUM_SHARDS = "0";
  RS_DIALECT = "postgres13";
}
