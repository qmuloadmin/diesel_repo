use std::{
    fmt::{Display, Formatter},
    ops::{Deref, DerefMut},
};

use actix_web::{error::BlockingError, web};
use async_trait::async_trait;
use diesel::{
    associations::HasTable,
    dsl::{Find, Limit},
    prelude::*,
    query_builder::{AsQuery, InsertStatement, IntoUpdateTarget, UpdateStatement},
    query_dsl::methods::{FindDsl, LimitDsl},
    query_dsl::{methods::FilterDsl, LoadQuery},
    r2d2::{ConnectionManager, PoolError},
    result::{DatabaseErrorKind, Error::DatabaseError, Error::NotFound},
    sql_types::Bool,
    PgConnection,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use r2d2::{Pool, PooledConnection};
use std::error::Error;

#[derive(Debug)]
pub struct RepoError {
    message: String,
    reason: Reason,
    cause: Option<Box<dyn Error + Send + Sync>>,
}

#[derive(Debug, PartialEq)]
pub enum Reason {
    Request,
    NoResults,
    Conflict,
    Other,
}

impl Display for RepoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("error: repo: {}", self.message))
    }
}

impl Error for RepoError {
    fn cause(&self) -> Option<&dyn Error> {
        match &self.cause {
            None => None,
            Some(b) => Some(&**b),
        }
    }
}

impl From<BlockingError> for RepoError {
    fn from(e: BlockingError) -> Self {
        RepoError {
            reason: Reason::Other,
            message: format!("{}", e),
            cause: Some(Box::new(e)),
        }
    }
}

impl From<diesel::result::Error> for RepoError {
    fn from(e: diesel::result::Error) -> Self {
        let reason = match e {
            NotFound => Reason::NoResults,
            DatabaseError(kind, _) => match kind {
                DatabaseErrorKind::UniqueViolation | DatabaseErrorKind::CheckViolation => {
                    Reason::Conflict
                }
                DatabaseErrorKind::ForeignKeyViolation => Reason::Request,
                _ => Reason::Other,
            },
            _ => Reason::Other,
        };
        RepoError {
            message: e.to_string(),
            reason,
            cause: Some(Box::new(e)),
        }
    }
}

impl From<PoolError> for RepoError {
    fn from(e: r2d2::Error) -> RepoError {
        RepoError::new_other(&format!(
            "error getting connection from pool: {}",
            e.to_string()
        ))
    }
}

impl From<RepoError> for jsonapi::Error {
    fn from(re: RepoError) -> Self {
        match re.reason {
            Reason::Conflict => jsonapi::Error::new_conflict(&re.message),
            Reason::NoResults => jsonapi::Error::new_not_found(&re.message),
            Reason::Request => jsonapi::Error::new_bad_request(&re.message),
            Reason::Other => jsonapi::Error::new_internal_error(&re.message),
        }
    }
}

impl RepoError {
    pub fn new_request(message: &str) -> Self {
        RepoError {
            reason: Reason::Request,
            message: format!("bad request: {}", message),
            cause: None,
        }
    }

    pub fn new_other(message: &str) -> Self {
        RepoError {
            reason: Reason::Other,
            message: format!("unknown error: {}", message),
            cause: None,
        }
    }

    pub fn new_conflict(message: &str) -> Self {
        RepoError {
            reason: Reason::Conflict,
            message: format!("conflict: {}", message),
            cause: None,
        }
    }
}

#[derive(Clone)]
/*
ConnPool wraps a r2d2 pool in an Rc to allow sharing between repositories.
Clone away (almost) freely
*/
pub struct ConnPool(Pool<ConnectionManager<PgConnection>>);

impl ConnPool {
    pub fn new(
        hostname: String,
        username: String,
        password: String,
        database: String,
        migs: EmbeddedMigrations,
    ) -> Self {
        let url = format!(
            "postgres://{}:{}@{}/{}",
            username, password, hostname, database
        );
        let mut mig_con = PgConnection::establish(&url).unwrap();
        println!("Running pending migrations");
        mig_con.run_pending_migrations(migs).unwrap();
        let manager = ConnectionManager::new(url);
        let pool =
            r2d2::Pool::new(manager).expect("Failed to create connection pool to PostgreSQL");
        Self(pool)
    }
}

impl Deref for ConnPool {
    type Target = Pool<ConnectionManager<PgConnection>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait Repository: Send + 'static
where
    Self: HasTable + Sized,
{
    type Resource: Send;
    type Id: Send + Clone;
    // Leave these as Unimplemented if needed and unimplemented. There's probably a better way to do this, but
    // for now, this unlocks default generics on the various child traits (e.g. CRRepository)
    // as well as auto implement generic traits
    type Update: Send;
    type Create: Send;

    fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, RepoError>;
}

// Use Unimplemented whenever you don't want to implement Update/Create for a Repository
pub enum Unimplemented {}

// Implement this trait to get all appropriate CRUD repo traits automatically implemented for you
// E.g. CRRepository will be implemented for your Create type.
// For simple resources where a resource is simply its row in the databse, this is usually enough.
pub trait AutoImplRepo: Repository {}

pub struct TxnContext<'txn, Baggage: Sized> {
    pub conn: &'txn mut PgConnection,
    pub baggage: Option<Baggage>,
}

pub struct Nothing {}

impl<'txn, Baggage> Deref for TxnContext<'txn, Baggage> {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        self.conn
    }
}

impl<'txn, Baggage> DerefMut for TxnContext<'txn, Baggage> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
    }
}

#[async_trait]
pub trait ActixWebCRepository<
    C = <Self as Repository>::Create,
    R = <Self as Repository>::Resource,
    B = Nothing,
>: CRepository<C, R, B> + Send where
    R: Send + 'static,
    C: Send + 'static,
    InsertStatement<Self::Table, <Self::Create as Insertable<Self::Table>>::Values>:
        LoadQuery<'static, PgConnection, Self::Resource>,
    Self::Create: Insertable<Self::Table>,
{
    async fn create(&self, new: C) -> Result<R, RepoError> {
        let mut conn = self.get_conn()?;
        web::block(move || Self::c(&mut conn, new)).await?
    }
}

#[async_trait]
pub trait ActixWebRRepository<R = <Self as Repository>::Resource>: RRepository<R> + Send
where
    R: Send + 'static,
    Self::Table: LimitDsl + FindDsl<Self::Id>,
    Find<Self::Table, Self::Id>: LimitDsl,
    Limit<Find<Self::Table, Self::Id>>: LoadQuery<'static, PgConnection, Self::Resource>,
{
    async fn find_by_id(&self, id: Self::Id) -> Result<R, RepoError> {
        let mut conn = self.get_conn()?;
        web::block(move || Self::r(&mut conn, id)).await?
    }
}

#[async_trait]
pub trait ActixWebURepository<
    U = <Self as Repository>::Update,
    R = <Self as Repository>::Resource,
    B = Nothing,
>: URepository<U, R, B> + Send where
    R: Send + 'static,
    U: Send + 'static,
    Self::Table: LimitDsl + FindDsl<Self::Id>, // to get a find method on table
    Find<Self::Table, Self::Id>: LimitDsl,     // to get find results
    Limit<Find<Self::Table, Self::Id>>: LoadQuery<'static, PgConnection, Self::Resource>, // for select fallback on no changes
    <Self::Table as FindDsl<Self::Id>>::Output: IntoUpdateTarget, // for update on find results
    UpdateStatement<
        <<Self::Table as FindDsl<Self::Id>>::Output as HasTable>::Table,
        <<Self::Table as FindDsl<Self::Id>>::Output as IntoUpdateTarget>::WhereClause,
        <Self::Update as AsChangeset>::Changeset,
    >: LoadQuery<'static, PgConnection, Self::Resource>, // For get_result
    UpdateStatement<
        <<Self::Table as FindDsl<Self::Id>>::Output as HasTable>::Table,
        <<Self::Table as FindDsl<Self::Id>>::Output as IntoUpdateTarget>::WhereClause,
        <Self::Update as AsChangeset>::Changeset,
    >: AsQuery, // for .set on update from find
    Self::Update:
        AsChangeset<Target = <<Self::Table as FindDsl<Self::Id>>::Output as HasTable>::Table>,
{
    async fn update(&self, id: Self::Id, changes: U) -> Result<R, RepoError> {
        let mut conn = self.get_conn()?;
        web::block(move || Self::u(&mut conn, id, changes)).await?
    }
}

#[async_trait]
pub trait ActixWebSearchableRepository<Terms: Send + 'static, R: Send + 'static>:
    SearchableRepository<Terms, R>
where
    Terms: IntoSearchTerms<Self::Table>,
    Self::Table: FilterDsl<SearchTerms<Self::Table>>,
    Self::Table: LoadQuery<'static, PgConnection, Self::Resource>,
    <Self::Table as FilterDsl<SearchTerms<Self::Table>>>::Output:
        LoadQuery<'static, PgConnection, Self::Resource>,
{
    async fn find_by_filter(&self, filter: Terms) -> Result<Vec<R>, RepoError> {
        let mut conn = self.get_conn()?;
        web::block(move || Self::search(&mut conn, filter)).await?
    }
}

impl<Repo: RRepository<R>, R: Send + 'static> ActixWebRRepository<R> for Repo
where
    Repo::Table: LimitDsl + FindDsl<Repo::Id>,
    Find<Repo::Table, Repo::Id>: LimitDsl,
    Limit<Find<Repo::Table, Repo::Id>>: LoadQuery<'static, PgConnection, Repo::Resource>,
{
}

impl<Repo: CRepository<C, R, B>, C: Send + 'static, R: Send + 'static, B>
    ActixWebCRepository<C, R, B> for Repo
where
    InsertStatement<Repo::Table, <Repo::Create as Insertable<Repo::Table>>::Values>:
        LoadQuery<'static, PgConnection, Repo::Resource>,
    Repo::Create: Insertable<Repo::Table>,
{
}

impl<Repo: URepository<U, R, B>, U: Send + 'static, R: Send + 'static, B>
    ActixWebURepository<U, R, B> for Repo
where
    Repo::Table: LimitDsl + FindDsl<Repo::Id>, // to get a find method on table
    Find<Repo::Table, Repo::Id>: LimitDsl,     // to get find results
    Limit<Find<Self::Table, Self::Id>>: LoadQuery<'static, PgConnection, Self::Resource>, // for select fallback on no changes
    <Repo::Table as FindDsl<Repo::Id>>::Output: IntoUpdateTarget, // for update on find results
    UpdateStatement<
        <<Repo::Table as FindDsl<Repo::Id>>::Output as HasTable>::Table,
        <<Repo::Table as FindDsl<Repo::Id>>::Output as IntoUpdateTarget>::WhereClause,
        <Repo::Update as AsChangeset>::Changeset,
    >: LoadQuery<'static, PgConnection, Repo::Resource>, // For get_result
    UpdateStatement<
        <<Repo::Table as FindDsl<Repo::Id>>::Output as HasTable>::Table,
        <<Repo::Table as FindDsl<Repo::Id>>::Output as IntoUpdateTarget>::WhereClause,
        <Repo::Update as AsChangeset>::Changeset,
    >: AsQuery, // for .set on update from find
    Repo::Update:
        AsChangeset<Target = <<Repo::Table as FindDsl<Repo::Id>>::Output as HasTable>::Table>,
{
}

impl<Repo: SearchableRepository<F, R>, F, R> ActixWebSearchableRepository<F, R> for Repo
where
    F: Send + 'static + IntoSearchTerms<Repo::Table>,
    R: Send + 'static,
    Repo::Table: FilterDsl<SearchTerms<Repo::Table>>,
    Repo::Table: LoadQuery<'static, PgConnection, Repo::Resource>,
    <Repo::Table as FilterDsl<SearchTerms<Repo::Table>>>::Output:
        LoadQuery<'static, PgConnection, Repo::Resource>,
{
}

pub trait CRepository<
    C = <Self as Repository>::Create,
    R = <Self as Repository>::Resource,
    Baggage = Nothing,
>: Repository + Sized where
    InsertStatement<Self::Table, <Self::Create as Insertable<Self::Table>>::Values>:
        LoadQuery<'static, PgConnection, Self::Resource>,
    Self::Create: Insertable<Self::Table>,
{
    fn pre_create(ctx: &mut TxnContext<Baggage>, new: C) -> Result<Self::Create, RepoError>;

    fn post_create(ctx: TxnContext<Baggage>, created: Self::Resource) -> Result<R, RepoError>;

    fn c(conn: &mut PgConnection, new: C) -> Result<R, RepoError> {
        Ok(conn.transaction(|conn| {
            let mut ctx = TxnContext {
                conn,
                baggage: None,
            };
            let new = Self::pre_create(&mut ctx, new)?;
            let resource = diesel::insert_into(Self::table())
                .values(new)
                .get_result(&mut ctx)?;
            Self::post_create(ctx, resource)
        })?)
    }
}

// Trait for converting from any type into a Resource trait type
// e.g. Resource::Create or Resource::Update
// This is essentially a from trait, but enriched to support performing database queries during the conversion
// to allow for needing to insert other related elements
pub trait IntoResourceType<R> {
    fn into_resource(self, ctx: &mut PgConnection) -> Result<R, RepoError>;
}

impl<R, T> IntoResourceType<R> for T
where
    R: From<T>,
{
    fn into_resource(self, _: &mut PgConnection) -> Result<R, RepoError> {
        Ok(R::from(self))
    }
}

pub trait FromResource<R>
where
    Self: Sized,
{
    fn from_resource(ctx: &mut PgConnection, res: R) -> Result<Self, RepoError>;
}

impl<R, T> FromResource<R> for T
where
    T: From<R>,
{
    fn from_resource(_: &mut PgConnection, res: R) -> Result<Self, RepoError> {
        Ok(Self::from(res))
    }
}

pub trait RRepository<R = <Self as Repository>::Resource>: Repository + Sized
where
    Self::Table: LimitDsl + FindDsl<Self::Id>,
    Find<Self::Table, Self::Id>: LimitDsl,
    Limit<Find<Self::Table, Self::Id>>: LoadQuery<'static, PgConnection, Self::Resource>,
{
    fn post_read(conn: &mut PgConnection, read: Self::Resource) -> Result<R, RepoError>;

    fn r(conn: &mut PgConnection, id: Self::Id) -> Result<R, RepoError> {
        Ok(conn.transaction(|conn| {
            let resource = FindDsl::find(Self::table(), id).limit(1).get_result(conn)?;
            Self::post_read(conn, resource)
        })?)
    }
}

impl<Repo: AutoImplRepo, R> RRepository<R> for Repo
where
    R: FromResource<Repo::Resource>,
    Repo::Table: LimitDsl + FindDsl<Repo::Id>,
    Find<Repo::Table, Repo::Id>: LimitDsl,
    Limit<Find<Repo::Table, Repo::Id>>: LoadQuery<'static, PgConnection, Repo::Resource>,
{
    fn post_read(conn: &mut PgConnection, read: Self::Resource) -> Result<R, RepoError> {
        Ok(R::from_resource(conn, read)?)
    }
}

impl<Repo: AutoImplRepo, C, R> CRepository<C, R, Nothing> for Repo
where
    R: FromResource<Repo::Resource>,
    C: IntoResourceType<Repo::Create>,
    InsertStatement<Repo::Table, <Repo::Create as Insertable<Repo::Table>>::Values>:
        LoadQuery<'static, PgConnection, Repo::Resource>,
    Repo::Create: Insertable<Repo::Table>,
{
    fn pre_create(conn: &mut TxnContext<Nothing>, new: C) -> Result<Self::Create, RepoError> {
        Ok(new.into_resource(&mut conn.conn)?)
    }

    fn post_create(conn: TxnContext<Nothing>, created: Self::Resource) -> Result<R, RepoError> {
        Ok(R::from_resource(conn.conn, created)?)
    }
}

pub trait SearchableRepository<
    Terms: IntoSearchTerms<Self::Table>,
    R = <Self as Repository>::Resource,
>: Repository + Sized where
    Self::Table: FilterDsl<SearchTerms<Self::Table>>,
    Self::Table: LoadQuery<'static, PgConnection, Self::Resource>,
    <Self::Table as FilterDsl<SearchTerms<Self::Table>>>::Output:
        LoadQuery<'static, PgConnection, Self::Resource>,
{
    fn search(conn: &mut PgConnection, terms: Terms) -> Result<Vec<R>, RepoError> {
        let term = terms.into_terms().into_iter().fold(
            Option::<SearchTerms<Self::Table>>::None,
            |terms, term| match terms {
                Some(terms) => Some(Box::new(terms.and(term))),
                None => Some(term),
            },
        );
        let results = match term {
            Some(term) => FilterDsl::filter(Self::table(), term).load(conn),
            None => Self::table().load(conn),
        }?;
        Self::post_search(conn, results)
    }

    fn post_search(
        conn: &mut PgConnection,
        results: Vec<Self::Resource>,
    ) -> Result<Vec<R>, RepoError>;
}

pub trait IntoSearchTerms<Table> {
    fn into_terms(self) -> Vec<SearchTerms<Table>>;
}

pub type SearchTerms<Table> = Box<dyn BoxableExpression<Table, diesel::pg::Pg, SqlType = Bool>>;

impl<Repo: AutoImplRepo, Terms, R> SearchableRepository<Terms, R> for Repo
where
    Terms: IntoSearchTerms<Self::Table>,
    R: FromResource<Self::Resource>,
    Repo::Table: FilterDsl<SearchTerms<Repo::Table>>,
    Repo::Table: LoadQuery<'static, PgConnection, Repo::Resource>,
    <Repo::Table as FilterDsl<SearchTerms<Repo::Table>>>::Output:
        LoadQuery<'static, PgConnection, Repo::Resource>,
{
    fn post_search(
        conn: &mut PgConnection,
        results: Vec<Self::Resource>,
    ) -> Result<Vec<R>, RepoError> {
        let mut converted = Vec::with_capacity(results.len());
        for each in results.into_iter() {
            converted.push(R::from_resource(conn, each)?);
        }
        Ok(converted)
    }
}

pub trait URepository<
    U = <Self as Repository>::Update,
    R = <Self as Repository>::Resource,
    Baggage = Nothing,
>: Repository + Sized where
    Self::Table: LimitDsl + FindDsl<Self::Id>, // to get a find method on table
    Find<Self::Table, Self::Id>: LimitDsl,     // to get find results
    Limit<Find<Self::Table, Self::Id>>: LoadQuery<'static, PgConnection, Self::Resource>, // for select fallback on no changes
    <Self::Table as FindDsl<Self::Id>>::Output: IntoUpdateTarget, // for update on find results
    UpdateStatement<
        <<Self::Table as FindDsl<Self::Id>>::Output as HasTable>::Table,
        <<Self::Table as FindDsl<Self::Id>>::Output as IntoUpdateTarget>::WhereClause,
        <Self::Update as AsChangeset>::Changeset,
    >: LoadQuery<'static, PgConnection, Self::Resource>, // For get_result
    UpdateStatement<
        <<Self::Table as FindDsl<Self::Id>>::Output as HasTable>::Table,
        <<Self::Table as FindDsl<Self::Id>>::Output as IntoUpdateTarget>::WhereClause,
        <Self::Update as AsChangeset>::Changeset,
    >: AsQuery, // for .set on update from find
    Self::Update:
        AsChangeset<Target = <<Self::Table as FindDsl<Self::Id>>::Output as HasTable>::Table>,
{
    fn pre_update(
        txn: &mut TxnContext<Baggage>,
        id: &Self::Id,
        changes: U,
    ) -> Result<Self::Update, RepoError>;

    fn post_update(txn: TxnContext<Baggage>, updated: Self::Resource) -> Result<R, RepoError>;

    fn u(conn: &mut PgConnection, id: Self::Id, changes: U) -> Result<R, RepoError> {
        Ok(conn.transaction(|conn| {
            let mut ctx = TxnContext {
                conn,
                baggage: None,
            };
            let changes = Self::pre_update(&mut ctx, &id, changes)?;
            let resource = match diesel::update(FindDsl::find(Self::table(), id.clone()))
                .set(changes)
                .get_result(&mut ctx)
            {
                Ok(resource) => resource,
                Err(diesel::result::Error::QueryBuilderError(_)) => {
                    FindDsl::find(Self::table(), id)
                        .limit(1)
                        .get_result(&mut ctx)?
                }
                Err(err) => return Err(err.into()),
            };
            Self::post_update(ctx, resource)
        })?)
    }
}

impl<Repo: AutoImplRepo, U, R> URepository<U, R, Nothing> for Repo
where
    U: IntoResourceType<Repo::Update>,
    R: FromResource<Repo::Resource>,
    Repo::Table: LimitDsl + FindDsl<Repo::Id>, // to get a find method on table
    Find<Repo::Table, Repo::Id>: LimitDsl,     // to get find results
    Limit<Find<Self::Table, Self::Id>>: LoadQuery<'static, PgConnection, Self::Resource>,
    <Repo::Table as FindDsl<Repo::Id>>::Output: IntoUpdateTarget, // for update on find results
    UpdateStatement<
        <<Repo::Table as FindDsl<Repo::Id>>::Output as HasTable>::Table,
        <<Repo::Table as FindDsl<Repo::Id>>::Output as IntoUpdateTarget>::WhereClause,
        <Repo::Update as AsChangeset>::Changeset,
    >: LoadQuery<'static, PgConnection, Repo::Resource>, // For get_result
    UpdateStatement<
        <<Repo::Table as FindDsl<Repo::Id>>::Output as HasTable>::Table,
        <<Repo::Table as FindDsl<Repo::Id>>::Output as IntoUpdateTarget>::WhereClause,
        <Repo::Update as AsChangeset>::Changeset,
    >: AsQuery, // for .set on update from find
    Repo::Update:
        AsChangeset<Target = <<Repo::Table as FindDsl<Repo::Id>>::Output as HasTable>::Table>,
{
    fn pre_update(
        conn: &mut TxnContext<Nothing>,
        _id: &Self::Id,
        changes: U,
    ) -> Result<Self::Update, RepoError> {
        changes.into_resource(conn)
    }

    fn post_update(conn: TxnContext<Nothing>, updated: Self::Resource) -> Result<R, RepoError> {
        R::from_resource(conn.conn, updated)
    }
}
