use crate::commands::Command;
use crate::context::AppContext;
use anyhow::Context;
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::str::FromStr;
use uuid::Uuid;
use valqeron_client::Client;
use valqeron_core::{
    Cnpj, CountryCode, Issuer, IssuerBuilder, IssuerId, IssuerName, IssuerStatus, Lei, Versioned,
};

#[derive(Subcommand, Debug)]
pub enum IssuerCommand {
    /// Register a new issuer.
    Register(RegisterArgs),

    /// List all issuers.
    List(ListArgs),

    /// Retrieve issuer info by id.
    Info(InfoArgs),
}

impl IssuerCommand {
    pub fn as_command(&self) -> &dyn Command {
        match self {
            IssuerCommand::Register(args) => args,
            IssuerCommand::List(args) => args,
            IssuerCommand::Info(args) => args,
        }
    }
}

// -------- Command Info --------
#[derive(Args, Debug)]
pub struct InfoArgs {
    /// Issuer id (UUID).
    #[arg(long)]
    pub id: String,
}

impl Command for InfoArgs {
    fn execute(&self, client: &Client, _ctx: &AppContext) -> anyhow::Result<Value> {
        let uuid =
            Uuid::from_str(&self.id).with_context(|| format!("invalid issuer id: {}", self.id))?;
        let id = IssuerId::from_uuid(uuid);

        let found = client.get_issuer(&id)?;
        let items: Vec<IssuerView> = found.iter().map(IssuerView::from).collect();

        tracing::info!(
            target: "valqeron::audit",
            operation = "issuer.info",
            id = %self.id,
            found = !items.is_empty(),
            "issuer lookup"
        );

        Ok(json!({
            "items": items,
            "count": items.len(),
        }))
    }
}

// -------- Command List --------

/// Default page size when `--limit` is not supplied.
const DEFAULT_LIMIT: u32 = 50;

/// Maximum allowable page size to prevent memory exhaustion.
const MAX_LIMIT: u32 = 1000;

#[derive(Args, Debug)]
pub struct ListArgs {
    #[arg(
        long,
        default_value_t = DEFAULT_LIMIT,
        value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_LIMIT)),
        help = &format!("Maximum number of issuers to return in this page (default: {})", DEFAULT_LIMIT)
    )]
    pub limit: u32,

    #[arg(
        long,
        help = "Return issuers whose id sorts after this one (UUID). Use the last id of the previous page to fetch the next page."
    )]
    pub after: Option<String>,
}

impl Command for ListArgs {
    fn execute(&self, client: &Client, _ctx: &AppContext) -> anyhow::Result<Value> {
        let after: Option<IssuerId> = match &self.after {
            Some(raw) => {
                let uuid = Uuid::from_str(raw)
                    .with_context(|| format!("invalid issuer pagination cursor: {raw}"))?;
                Some(IssuerId::from_uuid(uuid))
            }
            None => None,
        };

        let found = client.list_issuers(after.as_ref(), self.limit)?;
        let items: Vec<IssuerView> = found.iter().map(IssuerView::from).collect();

        // Cursor for the next page: the last id in this page, if any.
        let next_after: Option<String> = found.last().map(|v| v.data.id().value());

        tracing::info!(
            target: "valqeron::audit",
            operation = "issuer.list",
            count = items.len(),
            limit = self.limit,
            "list registered issuers (paged)"
        );

        Ok(json!({
            "items": items,
            "count": items.len(),
            "next_after": next_after,
        }))
    }
}

// -------- Command Register --------
#[derive(Args, Debug)]
pub struct RegisterArgs {
    #[arg(long, short = 'n', help = "Human-readable issuer name (required)")]
    pub name: Option<String>,

    #[arg(
        long,
        short = 's',
        help = "Issuer status (ACTIVE or RETIRED)",
        default_value = "ACTIVE"
    )]
    pub status: Option<String>,

    #[arg(long, help = "Brazilian CNPJ (punctuated or compact)")]
    pub cnpj: Option<String>,

    #[arg(long, help = "ISO 17442 LEI (punctuated or compact)")]
    pub lei: Option<String>,

    #[arg(long, help = "Country code (2-letter ISO 3166-1 alpha-2)")]
    pub country_code: Option<String>,
}

impl Command for RegisterArgs {
    fn execute(&self, client: &Client, ctx: &AppContext) -> anyhow::Result<Value> {
        let base: IssuerInput = match ctx.read_input()? {
            Some(value) => serde_json::from_value(value).context("invalid issuer input")?,
            None => IssuerInput::default(),
        };

        let input = base.merge_flags(
            self.name.clone(),
            self.status.clone(),
            self.cnpj.clone(),
            self.lei.clone(),
            self.country_code.clone(),
        );

        // Validate locally for instant feedback; the engine re-validates
        // authoritatively (its errors carry the same domain messages).
        let issuer = input.into_issuer()?;

        let registered = client.register_issuer(&issuer, ctx.dry_run())?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "issuer.register",
            id = %registered.data.id().value(),
            dry_run = ctx.dry_run(),
            "registered issuer"
        );

        let view = IssuerView::from(&registered);
        let payload = serde_json::to_value(view).context("failed to serialize issuer response")?;
        Ok(payload)
    }
}

// -------- DTO --------
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssuerInput {
    pub name: Option<String>,
    pub status: Option<String>,
    pub cnpj: Option<String>,
    pub lei: Option<String>,
    pub country_code: Option<String>,
}

impl IssuerInput {
    pub fn merge_flags(
        mut self,
        name: Option<String>,
        status: Option<String>,
        cnpj: Option<String>,
        lei: Option<String>,
        country_code: Option<String>,
    ) -> Self {
        if name.is_some() {
            self.name = name;
        }
        if status.is_some() {
            self.status = status;
        }
        if cnpj.is_some() {
            self.cnpj = cnpj;
        }
        if lei.is_some() {
            self.lei = lei;
        }
        if country_code.is_some() {
            self.country_code = country_code;
        }
        self
    }

    pub fn into_issuer(self) -> anyhow::Result<Issuer> {
        let mut builder: IssuerBuilder = Issuer::builder();

        if let Some(name) = self.name.as_deref() {
            builder = builder.name(IssuerName::new(name)?);
        }
        if let Some(status) = self.status.as_deref() {
            builder = builder.status(IssuerStatus::from_str(status)?);
        }
        if let Some(cnpj) = self.cnpj.as_deref() {
            builder = builder.cnpj(Cnpj::parse(cnpj)?);
        }
        if let Some(lei) = self.lei.as_deref() {
            builder = builder.lei(Lei::parse(lei)?);
        }
        if let Some(cc) = self.country_code.as_deref() {
            builder = builder.country_code(CountryCode::parse(cc)?);
        }

        Ok(builder.build()?)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct IssuerView {
    pub id: String,
    pub status: String,
    pub created_at: String,
    pub version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cnpj: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lei: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country_code: Option<String>,
}

impl IssuerView {
    pub fn new(issuer: &Issuer, version: u32) -> Self {
        Self {
            id: issuer.id().value(),
            status: String::from(issuer.status()),
            created_at: issuer.created_at().to_rfc3339(),
            version,
            name: issuer.name().map(|n| n.as_str().to_string()),
            cnpj: issuer.cnpj().map(|c| c.as_str().to_string()),
            lei: issuer.lei().map(|l| l.as_str().to_string()),
            country_code: issuer.country_code().map(|c| c.as_str().to_string()),
        }
    }
}

impl From<&Versioned<Issuer>> for IssuerView {
    fn from(v: &Versioned<Issuer>) -> Self {
        IssuerView::new(&v.data, v.version)
    }
}
