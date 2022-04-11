use std::include_str;

use crate::constants::{
    CONSUL_POSTFIX, IMG_PREFIX, MYSQL_POSTFIX, POSTGRES_POSTFIX, READYSET_MYSQL_POSTFIX,
    READYSET_POSTGRES_POSTFIX, READYSET_SERVER_POSTFIX, READYSET_TAG,
};
use crate::deployment::Engine;
use crate::docker_compose::Compose;

fn consul_img() -> String {
    format!("{}{}", IMG_PREFIX, CONSUL_POSTFIX)
}

fn mysql_img() -> String {
    format!("{}{}", IMG_PREFIX, MYSQL_POSTFIX)
}

fn postgres_img() -> String {
    format!("{}{}", IMG_PREFIX, POSTGRES_POSTFIX)
}

fn server_img() -> String {
    format!("{}{}:{}", IMG_PREFIX, READYSET_SERVER_POSTFIX, READYSET_TAG)
}

fn mysql_adapter_img() -> String {
    format!("{}{}:{}", IMG_PREFIX, READYSET_MYSQL_POSTFIX, READYSET_TAG)
}

fn postgres_adapter_img() -> String {
    format!(
        "{}{}:{}",
        IMG_PREFIX, READYSET_POSTGRES_POSTFIX, READYSET_TAG
    )
}

pub(crate) fn generate_base_template(db_type: &Engine, standalone: bool) -> Compose {
    match db_type {
        Engine::MySQL => generate_base_mysql_template(standalone),
        Engine::PostgreSQL => generate_base_postgres_template(standalone),
    }
}

fn generate_base_mysql_template(standalone: bool) -> Compose {
    let base_yml = if standalone {
        include_str!("./templates/base_mysql_standalone_template.yml")
    } else {
        include_str!("./templates/base_mysql_template.yml")
    };

    let mut template: Compose = serde_yaml::from_str::<Compose>(base_yml).unwrap();
    if let Some(ref mut services) = template.services {
        services.set_service_img("consul", consul_img());
        services.set_service_img("mysql", mysql_img());
        services.set_service_img("readyset-server", server_img());
        services.set_service_img("readyset-adapter", mysql_adapter_img());
    }
    template
}

fn generate_base_postgres_template(standalone: bool) -> Compose {
    let base_yml = if standalone {
        include_str!("./templates/base_pg_standalone_template.yml")
    } else {
        include_str!("./templates/base_pg_template.yml")
    };

    let mut template: Compose = serde_yaml::from_str::<Compose>(base_yml).unwrap();
    if let Some(ref mut services) = template.services {
        services.set_service_img("consul", consul_img());
        services.set_service_img("postgres", postgres_img());
        services.set_service_img("readyset-server", server_img());
        if !standalone {
            services.set_service_img("readyset-server", server_img());
        }
        services.set_service_img("readyset-adapter", postgres_adapter_img());
    }
    template
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::docker_compose::{Service, Services};

    /// Gets the service image with the provided name from `Services`, and asserts all the way down
    /// to retrieving said image.
    fn get_service_img(services: &mut HashMap<String, Option<Service>>, name: &str) -> String {
        let maybe_service = services.get(name);
        assert!(matches!(maybe_service, Some(Some(Service { .. }))));

        match maybe_service {
            Some(Some(Service {
                image: Some(img), ..
            })) => img.to_owned(),
            Some(Some(_)) => panic!("Missing image for service {}", name),
            _ => panic!("Service with name {} not found", name),
        }
    }

    #[test]
    fn images_match() {
        let want_consul = consul_img();
        let want_mysql = mysql_img();
        let want_server = server_img();
        let want_adapter = mysql_adapter_img();

        let mut compose = generate_base_template(&Engine::MySQL, false);

        let (got_consul, got_mysql, got_server, got_adapter) = match compose.services {
            Some(Services(ref mut map)) => (
                get_service_img(map, "consul"),
                get_service_img(map, "mysql"),
                get_service_img(map, "readyset-server"),
                get_service_img(map, "readyset-adapter"),
            ),
            _ => panic!("No services found"),
        };

        assert_eq!(got_consul, want_consul);
        assert_eq!(got_mysql, want_mysql);
        assert_eq!(got_server, want_server);
        assert_eq!(got_adapter, want_adapter);
    }
}
