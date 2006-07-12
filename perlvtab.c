
#define PERL_NO_GET_CONTEXT
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#ifdef MULTIPLICITY
#  define my_dTHX(a) pTHXx = ((PerlInterpreter*)(a))
#else
#  define my_dTHX(a) dNOOP
#endif

typedef struct _perl_vtab {
    sqlite3_vtab base;
    SV *sv;
    sqlite3 *db;
#ifdef MULTIPLICITY
    PerlInterpreter *perl;
#endif
} perl_vtab;

typedef struct _perl_vtab_cursor {
    sqlite3_vtab_cursor base;
    SV *sv;
} perl_vtab_cursor;

/*
static SV *
newSVwrap(pTHX_ char *class, void *obj) {
    SV *w = newSV(0);
    sv_setref_iv(w, class, PTR2IV(obj));
    return w;
}

static SV *
newSVsqlite3(pTHX_ sqlite3 *db) {
    return newSVwrap(aTHX_ "DBD::SQLite::_Internal::sqlite3", db);
}
*/


static int
perlCreateOrConnect(sqlite3 *db,
                    void *pAux,
                    int argc, char **argv,
                    sqlite3_vtab **ppVtab,
                    int create) {

    my_dTHX(pAux);
    dSP;
    I32 ax;
    int i;
    int count;
    SV *tmp;
    perl_vtab *vtab = NULL;
    SV *vtabsv;
    int rc = SQLITE_OK;

    if (argc < 4) {
        sqlite3Error(db, SQLITE_MISUSE, "perl driver name for virtual table missing");
        return SQLITE_ERROR;
    }

    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    XPUSHs(sv_2mortal(newSVpv("SQLite::VirtualTable", 0)));
    XPUSHs(sv_2mortal(newSVpv(create ? "CREATE" : "CONNECT", 0)));

    for (i = 0; i<argc; i++) {
        tmp = sv_2mortal(newSVpv(argv[i], 0));
        SvUTF8_on(tmp);
        XPUSHs(tmp);
    }

    PUTBACK;
    count = call_method("_CREATE_OR_CONNECT", G_SCALAR|G_EVAL);
    SPAGAIN;
    SP -= count;
    ax = (SP - PL_stack_base) + 1;
    vtabsv = ST(0);

    PUTBACK;

    if (!count || SvTRUE(ERRSV) || !SvOK(vtabsv)) {
        Perl_warn(aTHX_  "SQLite::VirtualTable::_%s method failed: %s\n",
                  create ? "CREATE" : "CONNECT",
                  SvTRUE(ERRSV) ? SvPVutf8_nolen(ERRSV) : "method returned undef");
        rc = SQLITE_ERROR;
        goto cleanup;
    }

    PUSHMARK(SP);
    XPUSHs(vtabsv);
    PUTBACK;
    count = call_method("DECLARE_SQL", G_SCALAR|G_EVAL);
    SPAGAIN;
    SP -= count;
    ax = (SP - PL_stack_base) + 1;
    tmp = ST(0);
    
    if (!count || SvTRUE(ERRSV) || !SvOK(tmp)) {
        sqlite3Error(db, SQLITE_ERROR, "%s::DECLARE_SQL method failed: %s",
                     sv_reftype(vtabsv, 1),
                     SvTRUE(ERRSV) ? SvPVutf8_nolen(ERRSV) : "method returned undef");
        rc = SQLITE_ERROR;
        goto cleanup;
    }

    rc = sqlite3_declare_vtab(db, SvPVutf8_nolen(tmp));
    if (rc != SQLITE_OK)
        goto cleanup;

    Newxz(vtab, 1, perl_vtab);
    vtab->sv = SvREFCNT_inc(vtabsv);
    vtab->db = db;
#ifdef MULTIPLICITY
    vtab->perl = my_perl;
#endif

cleanup:
    *ppVtab = (sqlite3_vtab *) vtab;

    FREETMPS;
    LEAVE;
    
    return rc;
}

static int
perlCreate(sqlite3 *db,
           void *pAux,
           int argc, char **argv,
           sqlite3_vtab **ppVtab) {
    return perlCreateOrConnect(db, pAux, argc, argv, ppVtab, 1);
}

static int
perlConnect(sqlite3 *db,
           void *pAux,
           int argc, char **argv,
           sqlite3_vtab **ppVtab) {
    return perlCreateOrConnect(db, pAux, argc, argv, ppVtab, 0);
}

static int
perlDestroyOrDisconnect(sqlite3_vtab *vtab, int destroy) {
    my_dTHX(((perl_vtab*)vtab)->perl);
    dSP;
    SV *vtabsv = ((perl_vtab*)vtab)->sv;
    int count;
    int rc = SQLITE_OK;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    PUSHs(vtabsv);
    PUTBACK;
    count = call_method(destroy ? "DROP" : "DISCONNECT", G_VOID|G_EVAL);
    SPAGAIN;
    SP -= count;
    PUTBACK;

    if (SvTRUE(ERRSV)) {
        sqlite3Error(((perl_vtab *)vtab)->db,
                     SQLITE_ERROR, "%s::%s method failed: %s",
                     sv_reftype(vtabsv, 1),
                     destroy ? "DROP" : "DISCONNECT",
                     SvPVutf8_nolen(ERRSV));
        rc = SQLITE_ERROR;
        goto cleanup;
    }

    SvREFCNT_dec(vtabsv);
    Safefree(vtab);
    
cleanup:
    FREETMPS;
    LEAVE;

    return rc;
}

static int
perlDestroy(sqlite3_vtab *vtab) {
    return perlDestroyOrDisconnect(vtab, 1);
}

static int
perlDisconnect(sqlite3_vtab *vtab) {
    return perlDestroyOrDisconnect(vtab, 0);
}


static int
perlOpen(sqlite3_vtab *vtab, sqlite3_vtab_cursor **ppCursor) {
    my_dTHX(((perl_vtab *)vtab)->perl);
    dSP;
    I32 ax;
    perl_vtab_cursor *cursor = NULL;
    SV *vtabsv = ((perl_vtab*)vtab)->sv;
    SV *cursv;
    int count;
    int rc = SQLITE_OK;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    PUSHs(vtabsv);
    PUTBACK;
    count = call_method("OPEN", G_SCALAR|G_EVAL);
    SPAGAIN;

    SP -= count;
    ax = (SP - PL_stack_base) + 1;
    cursv = ST(0);
    PUTBACK;

    if (!count || !SvOK(cursv)) {
        sqlite3Error(((perl_vtab *)vtab)->db,
                     SQLITE_ERROR, "%s::OPEN method failed: %s",
                     sv_reftype(vtabsv, 1),
                     SvTRUE(ERRSV) ? SvPVutf8_nolen(ERRSV) : "method returned undef");
        rc = SQLITE_ERROR;
        goto cleanup;
    }

    Newxz(cursor, 1, perl_vtab_cursor);
    cursor->sv = SvREFCNT_inc(cursv);
    SvREFCNT_inc(vtabsv);

cleanup:
    *ppCursor = (sqlite3_vtab_cursor *) cursor;

    FREETMPS;
    LEAVE;

    return rc;
}

static int
perlClose(sqlite3_vtab_cursor *cur) {
    SV *cursv = ((perl_vtab_cursor *)cur)->sv;
    perl_vtab *vtab = (perl_vtab *)(cur->pVtab);
    my_dTHX(((perl_vtab*)vtab)->perl);
    dSP;
    SV *vtabsv = vtab->sv;
    int count;
    int rc = SQLITE_OK;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    PUSHs(vtabsv);
    PUSHs(cursv);

    PUTBACK;
    count = call_method("CLOSE", G_VOID|G_EVAL);
    SPAGAIN;

    SP -= count;
    PUTBACK;

    if (SvTRUE(ERRSV)) {
        sqlite3Error(((perl_vtab *)vtab)->db,
                     SQLITE_ERROR, "%s::CLOSE method failed: %s",
                     sv_reftype(vtabsv, 1), SvPVutf8_nolen(ERRSV));
        rc = SQLITE_ERROR;
        goto cleanup;
    }
    
    SvREFCNT_dec(cursv);
    SvREFCNT_dec(vtabsv);
    Safefree(cur);

cleanup:
    FREETMPS;
    LEAVE;

    return rc;
}

static char *
op2str(unsigned char op) {
    switch (op) {
    case SQLITE_INDEX_CONSTRAINT_EQ:
        return "eq";
    case SQLITE_INDEX_CONSTRAINT_GT:
        return "gt";
    case SQLITE_INDEX_CONSTRAINT_LE:
        return "le";
    case SQLITE_INDEX_CONSTRAINT_LT:
        return "lt";
    case SQLITE_INDEX_CONSTRAINT_GE:
        return "ge";
    case SQLITE_INDEX_CONSTRAINT_MATCH:
        return "match";
    default:
        return "unknown";
    }
}

int perlBestIndex(sqlite3_vtab *vtab, sqlite3_index_info *ixinfo) {
    my_dTHX(((perl_vtab*)vtab)->perl);
    dSP;
    I32 ax;
    SV *vtabsv = ((perl_vtab*)vtab)->sv;
    AV *av;
    AV *ctrain;
    int count;
    int i;
    int len;
    char *str;
    int rc = SQLITE_OK;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    PUSHs(vtabsv);

    ctrain = newAV();
    PUSHs(sv_2mortal(newRV_noinc((SV*)ctrain)));

    for (i = 0; i < ixinfo->nConstraint; i++) {
        HV *hv = newHV();
        av_push(ctrain, newRV_noinc((SV*)hv));
        hv_store(hv, "column",  6, newSViv(ixinfo->aConstraint[i].iColumn), 0);
        hv_store(hv, "operator", 8, newSVpv(op2str(ixinfo->aConstraint[i].op), 0), 0);
        hv_store(hv, "usable", 6, (ixinfo->aConstraint[i].usable ? &PL_sv_yes : &PL_sv_no), 0);
    }

    av = newAV();
    PUSHs(sv_2mortal(newRV_noinc((SV*)av)));

    for (i = 0; i < ixinfo->nOrderBy; i++) {
        HV *hv = newHV();
        av_push(av, newRV_noinc((SV*)hv));
        hv_store(hv, "column",  6, newSViv(ixinfo->aOrderBy[i].iColumn), 0);
        hv_store(hv, "direction", 9, newSViv(ixinfo->aOrderBy[i].desc ? -1 : 1), 0);
    }
    
    PUTBACK;
    count = call_method("BEST_INDEX", G_ARRAY|G_EVAL);
    SPAGAIN;

    SP -= count;
    ax = (SP - PL_stack_base) + 1;

    PUTBACK;

    if (SvTRUE(ERRSV)) {
        Perl_warn(aTHX_ "%s::BEST_INDEX method failed: %s\n",
                  sv_reftype(vtabsv, 1),
                  SvPV_nolen(ERRSV));
        rc = SQLITE_ERROR;
        goto cleanup;
    }

    if (count != 4) {
        Perl_warn(aTHX_ "%s::BEST_INDEX method returned wrong number of values (%d, %d expected)",
                  sv_reftype(vtabsv, 1), count, 4);
        rc = SQLITE_ERROR;
        goto cleanup;
    }

    ixinfo->idxNum = SvIV(ST(0));
    str = SvPVutf8(ST(1), len);
    ixinfo->idxStr = sqlite3_malloc(len+1);
    memcpy(ixinfo->idxStr, str, len);
    ixinfo->idxStr[len] = 0;
    ixinfo->needToFreeIdxStr = 1;

    ixinfo->orderByConsumed = SvTRUE(ST(2));
    ixinfo->estimatedCost = SvNV(ST(3));

    for (i = 0; i < ixinfo->nConstraint; i++) {
        SV **rv = av_fetch(ctrain, i, FALSE);
        if (rv && SvROK(*rv) && SvTYPE(SvRV(*rv)) == SVt_PVHV) {
            HV *hv = (HV*)SvRV(*rv);
            SV **val;
            val = hv_fetch(hv, "arg_index", 9, FALSE);
            ixinfo->aConstraintUsage[i].argvIndex = (val && SvOK(*val)) ? SvIV(*val) + 1 : 0;
            val = hv_fetch(hv, "omit", 4, FALSE);
            ixinfo->aConstraintUsage[i].omit = (val && SvTRUE(*val)) ? 1 : 0;
            /* Perl_warn(aTHX_ "omit: %d\n", ixinfo->aConstraintUsage[i].omit); */
        }
        else {
            Perl_warn(aTHX_ "%s::BEST_INDEX method has corrupted constraint data structure",
                      sv_reftype(vtabsv, 1));
            rc = SQLITE_ERROR;
            goto cleanup;
        }
    }

cleanup:
    FREETMPS;
    LEAVE;
    return rc;
}

static int
perlEof(sqlite3_vtab_cursor* cur) {
    I32 ax;
    SV *cursv = ((perl_vtab_cursor *)cur)->sv;
    perl_vtab *vtab = (perl_vtab *)(cur->pVtab);
    my_dTHX(((perl_vtab*)vtab)->perl);
    dSP;
    SV *vtabsv = vtab->sv;
    SV *rcsv;
    int count;
    int rc = 0;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    PUSHs(vtabsv);
    PUSHs(cursv);

    PUTBACK;
    count = call_method("EOF", G_SCALAR|G_EVAL);
    SPAGAIN;

    SP -= count;
    ax = (SP - PL_stack_base) + 1;
    rcsv = ST(0);
    PUTBACK;

    if (SvTRUE(ERRSV)) {
        sqlite3Error(((perl_vtab *)vtab)->db,
                     SQLITE_ERROR, "%s::EOF method failed: %s",
                     sv_reftype(vtabsv, 1), SvPVutf8_nolen(ERRSV));
        rc = 1;
        goto cleanup;
    }

    rc = SvTRUE(rcsv);

cleanup:
    FREETMPS;
    LEAVE;
    return rc;
}

static int
perlNext(sqlite3_vtab_cursor* cur) {
    SV *cursv = ((perl_vtab_cursor *)cur)->sv;
    perl_vtab *vtab = (perl_vtab *)(cur->pVtab);
    my_dTHX(((perl_vtab*)vtab)->perl);
    dSP;
    SV *vtabsv = vtab->sv;
    SV *rcsv;
    int count;
    int rc = SQLITE_OK;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    PUSHs(vtabsv);
    PUSHs(cursv);
    PUTBACK;
    count = call_method("NEXT", G_SCALAR|G_EVAL);
    SPAGAIN;

    SP -= count;
    PUTBACK;

    if (SvTRUE(ERRSV)) {
        sqlite3Error(((perl_vtab *)vtab)->db,
                     SQLITE_ERROR, "%s::NEXT method failed: %s",
                     sv_reftype(vtabsv, 1), SvPVutf8_nolen(ERRSV));
        rc = SQLITE_ERROR;
    }

cleanup:
    FREETMPS;
    LEAVE;
    return rc;
}

static int
perlColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int n) {
    I32 ax;
    SV *cursv = ((perl_vtab_cursor *)cur)->sv;
    perl_vtab *vtab = (perl_vtab *)(cur->pVtab);
    my_dTHX(((perl_vtab*)vtab)->perl);
    dSP;
    SV *vtabsv = vtab->sv;
    SV *sv;
    int count;
    int rc = SQLITE_OK;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    PUSHs(vtabsv);
    PUSHs(cursv);
    PUSHs(sv_2mortal(newSViv(n)));
    PUTBACK;
    count = call_method("COLUMN", G_SCALAR|G_EVAL);
    SPAGAIN;

    SP -= count;
    ax = (SP - PL_stack_base) + 1;
    sv = ST(0);
    PUTBACK;

    if (SvTRUE(ERRSV)) {
        STRLEN len;
        char *str;
        SV *err = sv_2mortal(newSVpvf("%s::COLUMN method failed: %s",
                                      sv_reftype(vtabsv, 1),
                                      SvPVutf8_nolen(ERRSV)));
        str = SvPV(err, len);
        sqlite3_result_error(ctx, str, len);
        rc = SQLITE_ERROR;
        goto cleanup;
    }

    if (!SvOK(sv)) {
        /* Perl_warn(aTHX_ "undef found"); */
        sqlite3_result_null(ctx);
    }
    else if (SvIOK(sv)) {
        /* Perl_warn(aTHX_ "int found"); */
        sqlite3_result_int(ctx, SvIV(sv));
    }
    else if (SvNOK(sv)) {
        /* Perl_warn(aTHX_ "number found"); */
        sqlite3_result_double(ctx, SvNV(sv));
    }
    else {
        STRLEN len;
        char *str = SvPVutf8(sv, len);
        /* Perl_warn(aTHX_ "string found"); */
        sqlite3_result_text(ctx, str, len, SQLITE_TRANSIENT);
    }


cleanup:
    FREETMPS;
    LEAVE;
    return rc;
}

static SV *
newSVsqlite3_value(pTHX_ sqlite3_value *v) {
    SV *sv;
    int type = sqlite3_value_type(v);
    switch(type) {
    case SQLITE_NULL:
        return &PL_sv_undef;

    case SQLITE_INTEGER:
        return newSViv(sqlite3_value_int(v));

    case SQLITE_FLOAT:
        return newSVnv(sqlite3_value_double(v));

    case SQLITE_TEXT:
        sv = newSVpvn(sqlite3_value_text(v),
                      sqlite3_value_bytes(v));
        SvUTF8_on(sv);
        return sv;

    case SQLITE_BLOB:
        return newSVpvn((char *)sqlite3_value_text(v),
                        sqlite3_value_bytes(v));
    }
    Perl_warn(aTHX_ "unsupported SQLite type %d found", type);
    return &PL_sv_undef;
}

static int
perlFilter(sqlite3_vtab_cursor *cur,
           int idxNum, const char *idxStr,
           int argc, sqlite3_value **argv) {
    
    SV *cursv = ((perl_vtab_cursor *)cur)->sv;
    perl_vtab *vtab = (perl_vtab *)(cur->pVtab);
    my_dTHX(((perl_vtab*)vtab)->perl);
    dSP;
    SV *vtabsv = vtab->sv;
    SV *tmp;
    int i;
    int count;
    int rc = SQLITE_OK;

    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    PUSHs(vtabsv);
    PUSHs(cursv);
    PUSHs(sv_2mortal(newSViv(idxNum)));
    tmp = sv_2mortal(newSVpv(idxStr, 0));
    SvUTF8_on(tmp);
    PUSHs(tmp);
    for (i = 0; i < argc; i++)
        PUSHs(sv_2mortal(newSVsqlite3_value(aTHX_ argv[i])));
    PUTBACK;
    count = call_method("FILTER", G_VOID|G_EVAL);
    SPAGAIN;
    SP -= count;
    PUTBACK;

    if (SvTRUE(ERRSV)) {
        sqlite3Error(((perl_vtab *)vtab)->db,
                     SQLITE_ERROR, "%s::FILTER method failed: %s",
                     sv_reftype(vtabsv, 1),
                     SvPVutf8_nolen(ERRSV));
        rc = SQLITE_ERROR;
    }

cleanup:
    FREETMPS;
    LEAVE;
    return rc;
}

static int
perlRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *rowid) {
    SV *cursv = ((perl_vtab_cursor *)cur)->sv;
    perl_vtab *vtab = (perl_vtab *)(cur->pVtab);
    my_dTHX(((perl_vtab*)vtab)->perl);
    dSP;
    SV *vtabsv = vtab->sv;
    SV *rowidsv;
    I32 ax;
    int i;
    int count;
    int rc = SQLITE_OK;

    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    PUSHs(vtabsv);
    PUSHs(cursv);

    PUTBACK;
    count = call_method("ROWID", G_SCALAR|G_EVAL);
    SPAGAIN;
    SP -= count;
    rowidsv = ST(0);
    PUTBACK;

    if (!count || SvTRUE(ERRSV) || !SvOK(rowidsv)) {
        sqlite3Error(((perl_vtab *)vtab)->db,
                     SQLITE_ERROR, "%s::ROWID method failed: %s",
                     sv_reftype(vtabsv, 1),
                     SvTRUE(ERRSV) ? SvPVutf8_nolen(ERRSV) : "method returned undef");
        rc = SQLITE_ERROR;
        goto cleanup;
    }

    if (SvUOK(rowidsv))
        *rowid = SvUV(rowidsv);
    else if (SvIOK(rowidsv))
        *rowid = SvIV(rowidsv);
    else
        *rowid = SvNV(rowidsv);

cleanup:
    FREETMPS;
    LEAVE;
    return rc;
}


static int
perlUpdate(sqlite3_vtab *vtab) {
    my_dTHX(((perl_vtab*)vtab)->perl);
    Perl_warn(aTHX_ "UPDATE not implemented\n");
    return SQLITE_ERROR;
}

static int
perlBegin(sqlite3_vtab *vtab) {
    my_dTHX(((perl_vtab*)vtab)->perl);
    Perl_warn(aTHX_ "BEGIN not implemented\n");
    return SQLITE_OK;
}

static int
perlSync(sqlite3_vtab *vtab) {
    my_dTHX(((perl_vtab*)vtab)->perl);
    Perl_warn(aTHX_ "SYNC not implemented\n");
    return SQLITE_OK;
}


static int
perlCommit(sqlite3_vtab *vtab) {
    my_dTHX(((perl_vtab*)vtab)->perl);
    Perl_warn(aTHX_ "COMMIT not implemented\n");
    return SQLITE_OK;
}

static int
perlRollback(sqlite3_vtab *vtab) {
    my_dTHX(((perl_vtab*)vtab)->perl);
    Perl_warn(aTHX_ "ROLLBACK not implemented\n");
    return SQLITE_OK;
}

sqlite3_module perlModule = {
    0,
    perlCreate,
    perlConnect,
    perlBestIndex,
    perlDisconnect,
    perlDestroy,
    perlOpen,
    perlClose,
    perlFilter,
    perlNext,
    perlEof,
    perlColumn,
    perlRowid,
    perlUpdate,
    perlBegin,
    perlSync,
    perlCommit,
    perlRollback,
};

static char *argv[] = { "perlvtab",
                        "-e",
                        "require SQLite::VirtualTable;\n",
                        NULL };

EXTERN_C void boot_DynaLoader (pTHX_ CV* cv);

static void
xs_init(pTHX) {
    char *file = __FILE__;
    newXS("DynaLoader::boot_DynaLoader", boot_DynaLoader, file);
}

int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg, 
                           const sqlite3_api_routines *pApi) {

    PerlInterpreter *my_perl = perl_alloc();
    PERL_SYS_INIT3(3, argv, NULL);
    perl_construct(my_perl);
    perl_parse(my_perl, xs_init, 3, argv, environ);
    perl_run(my_perl);

    SQLITE_EXTENSION_INIT2(pApi)

    sqlite3_create_module(db, "perl", &perlModule, my_perl);
    return SQLITE_OK;
}
