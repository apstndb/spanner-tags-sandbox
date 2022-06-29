package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apstndb/spanner-tags-sandbox/spannerplus"

	"cloud.google.com/go/spanner"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalln(err)
	}
}

func nop(_ *spanner.Row) error {
	return nil
}

func run(ctx context.Context) error {
	project := flag.String("project", "", "")
	instance := flag.String("instance", "", "")
	database := flag.String("database", "", "")
	flag.Parse()

	rawClient, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", *project, *instance, *database))
	if err != nil {
		return err
	}
	client := spannerplus.WrapClient(rawClient, []spannerplus.Tag{{"app", "tag-sandbox"}})
	defer client.Close()

	tx := client.ReadOnlyTransaction([]spannerplus.Tag{{"tx", "ro1"}})
	defer tx.Close()

	// app=tag-sandbox,tx=ro1,h=1bb5077c
	if err = tx.QueryWithHash(ctx, spanner.NewStatement("SELECT 1")).Do(nop); err != nil {
		return err
	}

	// app=tag-sandbox,tx=ro1,h=9087fd8b
	if err = tx.QueryWithHash(ctx, spanner.NewStatement("SELECT 2")).Do(nop); err != nil {
		return err
	}

	// app=tag-sandbox,tx=ro1,query=FindAllSingers
	if err = tx.QueryWithTags(ctx, spanner.NewStatement("SELECT * FROM Singers"), spannerplus.Tag{"query", "FindAllSingers"}).Do(nop); err != nil {
		return err
	}

	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		// app=tag-sandbox,tx=rw1,
		_, err = client.ReadWriteTransaction(ctx, []spannerplus.Tag{{"tx", "rw1"}},
			func(ctx context.Context, transaction *spannerplus.ReadWriteTransactionWrapper) error {
				// app=tag-sandbox,tx=rw1,h=f0e76052
				err := transaction.QueryWithHash(ctx, spanner.NewStatement("@{LOCK_SCANNED_RANGES=exclusive} SELECT FirstName, LastName FROM Singers")).Do(nop)
				time.Sleep(1 * time.Second)
				return err
			})
		return err
	})

	grp.Go(func() error {
		// app=tag-sandbox,tx=rw2
		_, err = client.ReadWriteTransactionTagString(ctx, "tx=rw2",
			func(ctx context.Context, transaction *spannerplus.ReadWriteTransactionWrapper) error {
				// app=tag-sandbox,tx=rw2,h=f0e76052
				err := transaction.QueryWithHash(ctx,
					spanner.NewStatement("@{LOCK_SCANNED_RANGES=exclusive} SELECT FirstName, LastName FROM Singers")).Do(nop)
				time.Sleep(1 * time.Second)
				return err
			})
		return err
	})

	grp.Go(func() error {
		// app=tag-sandbox,tx=rw3
		_, err = client.ReadWriteTransaction(ctx, []spannerplus.Tag{{"tx", "rw3"}},
			func(ctx context.Context, transaction *spannerplus.ReadWriteTransactionWrapper) error {
				// app=tag-sandbox,tx=rw3,h=69daf312
				err := transaction.QueryWithHash(ctx,
					spanner.NewStatement("@{LOCK_SCANNED_RANGES=exclusive} SELECT BirthDate FROM Singers")).Do(nop)
				time.Sleep(1 * time.Second)
				return err
			})
		return err
	})
	return grp.Wait()
}
